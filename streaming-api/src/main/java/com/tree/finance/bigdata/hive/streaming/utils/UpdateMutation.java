package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.constants.DynamicConfig;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.utils.common.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_RECORDID_TBL_SUFFIX;
import static com.tree.finance.bigdata.schema.SchemaConstants.FIELD_KEY;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 10:33
 */
public class UpdateMutation extends Mutation {

    private static Logger LOG = LoggerFactory.getLogger(UpdateMutation.class);

    private HiveConf conf;

    TreeMap<RecordIdentifier, GenericData.Record> recordIdsortedRecord = new TreeMap<>();
    Object2ObjectOpenHashMap<RecordIdentifier, String> recordIdToBuziId = new Object2ObjectOpenHashMap();
    Object2LongOpenHashMap<RecordIdentifier> recordToUpdateTime = new Object2LongOpenHashMap<>();

    private List<GenericData.Record> toUpdate = new ArrayList<>();

    public UpdateMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }


    public void lazyUpdate(GenericData.Record record) throws Exception {
        toUpdate.add(record);
    }

    private void doUpdate() throws IOException, DataDelayedException {
        if (this.hbaseUtils == null) {
            this.hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX, hbaseConf);
        }
        List<Get> getReqs = new ArrayList<>();
        List<String> businessIds = new ArrayList<>();
        for (GenericData.Record record : toUpdate) {
            GenericData.Record keyRecord = (GenericData.Record) record.get(FIELD_KEY);
            String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(FIELD_KEY).schema());
            businessIds.add(businessId);
            getReqs.add(new Get(Bytes.toBytes(businessId)));
        }

        long start = System.currentTimeMillis();
        Result[] results = hbaseUtils.getAll(getReqs);
        LOG.info("HBase batch get cost: {}ms", System.currentTimeMillis() - start);

        if (results.length != getReqs.size()) {
            throw new DataDelayedException("HBase get no expected result size");
        }

        for (int i = 0; i < results.length; i++) {
            Result result = results[i];

            if (result.isEmpty()) {
                throw new DataDelayedException("not recId found for: " + businessIds.get(i));
            }

            byte[] idBytes = result.getValue(columnFamily, recordIdColIdentifier);
            byte[] timeBytes = result.getValue(columnFamily, updateTimeColIdentifier);

            //recordId= transactionId_BUCKET_ID _rowId
            String recordId = null == idBytes ? null : Bytes.toString(idBytes);

            if (null == recordId) {
                LOG.warn("no recordId in HBase: {}", businessIds.get(i));
                throw new DataDelayedException("");
            }

            Long hbaseTime = null == timeBytes ? null : Bytes.toLong(timeBytes);

            Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(updateCol, toUpdate.get(i));

            if (null != hbaseTime && recordUpdateTime <= hbaseTime) {
                continue;
            }

            if (recordToUpdateTime.containsKey(recordId) && recordToUpdateTime.get(recordId) > recordUpdateTime) {
                return;
            }

            String[] recordIds = RecordUtils.splitRecordId(recordId, '_');
            RecordIdentifier recordIdentifier = new RecordIdentifier(Integer.valueOf(recordIds[0]),
                    Integer.valueOf(recordIds[1]), Integer.valueOf(recordIds[2]));
            recordIdsortedRecord.put(recordIdentifier, toUpdate.get(i));

            bizToRecIdMap.put(businessIds.get(i), recordIdentifier);
            recordIdToBuziId.put(recordIdentifier, businessIds.get(i));
            recordToUpdateTime.put(recordIdentifier, recordUpdateTime);
        }
    }

    /*private void update(GenericData.Record record, boolean ignoreNotExist) throws Exception {
        if (this.hbaseUtils == null) {
            this.hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX, hbaseConf);
        }
        GenericData.Record keyRecord = (GenericData.Record) record.get(FIELD_KEY);

        String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(FIELD_KEY).schema());

        Object[] idAndTime = hbaseUtils.getAsBytes(businessId, columnFamily, recordIdColIdentifier, updateTimeColIdentifier);

        //recordId= transactionId_BUCKET_ID _rowId
        String recordId = null == idAndTime[0] ? null : Bytes.toString((byte[]) idAndTime[0]);
        Long hbaseTime = null == idAndTime[1] ? null : Bytes.toLong((byte[]) idAndTime[1]);

        if (!ignoreNotExist) {
            if (StringUtils.isEmpty(recordId)) {
                LOG.warn("no recordId found for: {}, data maybe delayed", businessId);
                throw new DataDelayedException("no recordId found for " + businessId);
            }
        }

        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(updateCol, record);

        if (null != hbaseTime && recordUpdateTime <= hbaseTime) {
            return;
        }

        if (recordToUpdateTime.containsKey(recordId) && recordToUpdateTime.get(recordId) > recordUpdateTime) {
            return;
        }

        String[] recordIds = RecordUtils.splitRecordId(recordId, '_');
        RecordIdentifier recordIdentifier = new RecordIdentifier(Integer.valueOf(recordIds[0]),
                Integer.valueOf(recordIds[1]), Integer.valueOf(recordIds[2]));
        recordIdsortedRecord.put(recordIdentifier, record);

        recordIdToBuziId.put(recordIdentifier, businessId);
        recordToUpdateTime.put(recordIdentifier, recordUpdateTime);
    }*/

    @Override
    public void beginStreamTransaction(Schema schema, HiveConf hiveConf) throws Exception{
        this.recordSchema = schema;
        this.checkExist = true;
        this.conf = hiveConf;
        super.beginTransaction(schema, hiveConf);
    }

    @Override
    public void commitTransaction() throws Exception {

        if (toUpdate.isEmpty()) {
            super.commitTransaction();
            return;
        }

        doUpdate();

        if (recordIdsortedRecord.isEmpty()) {
            super.commitTransaction();
            return;
        }

        long writeStart = System.currentTimeMillis();

        for (Map.Entry<RecordIdentifier, GenericData.Record> entry : recordIdsortedRecord.entrySet()) {
            GenericData.Record record = entry.getValue();
            if (record == null) {
                continue;
            }
            if (Operation.DELETE.code().equals(record.get("op").toString())) {
                mutateCoordinator.delete(partitions, record);
            } else {
                mutateCoordinator.update(partitions, record);
            }

            Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(updateCol, record);
            if (null == latestUpdateTime) {
                this.latestUpdateTime = recordUpdateTime;
            } else if (this.latestUpdateTime < recordUpdateTime) {
                this.latestUpdateTime = recordUpdateTime;
            }

            Put put = new Put(Bytes.toBytes(recordIdToBuziId.get(entry.getKey())));
            if (null != recordUpdateTime) {
                put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
            }
            hbaseUtils.insertAsync(put);
        }

        LOG.info("update write finished, cost: {}ms", System.currentTimeMillis() - writeStart);

        long commitStart = System.currentTimeMillis();
        super.commitTransaction();
        LOG.info("commit success cost: {}", System.currentTimeMillis() - commitStart);
    }
}
