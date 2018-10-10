package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.task.Operation;
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

    private boolean insertNoExist = false;

    TreeMap<RecordIdentifier, GenericData.Record> recordIdsortedRecord = new TreeMap<>();
    Object2ObjectOpenHashMap<RecordIdentifier, String> recordIdToBuziId = new Object2ObjectOpenHashMap();
    Object2LongOpenHashMap<RecordIdentifier> recordIdToUpdateTime = new Object2LongOpenHashMap<>();
    Object2LongOpenHashMap<String> bizIdToUpdateTime = new Object2LongOpenHashMap<>();

    List<String> businessIds = new ArrayList<>();
    List<Get> getReqs = new ArrayList<>();

    private List<GenericData.Record> toUpdate = new ArrayList<>();

    public UpdateMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }

    public UpdateMutation(String db, String table, String partition, List<String> partitions, String metastoreUris
            , Configuration hbaseConf, boolean insertNoExist) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
        this.insertNoExist = insertNoExist;
    }

    public void update(GenericData.Record record) throws Exception {
        GenericData.Record keyRecord = (GenericData.Record) record.get(FIELD_KEY);
        String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(FIELD_KEY).schema());

        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(updateCol, record);
        if (recordUpdateTime == 0L) {
            LOG.warn("record update time is null or zero, id: {}", businessId);
        }
        //skip the same record of older version in the same transaction
        if (bizIdToUpdateTime.containsKey(businessId) && bizIdToUpdateTime.get(businessId) >= recordUpdateTime) {
            return;
        }
        getReqs.add(new Get(Bytes.toBytes(businessId)));
        bizIdToUpdateTime.put(businessId, recordUpdateTime);
        businessIds.add(businessId);
        toUpdate.add(record);
    }

    /**
     * @throws IOException
     * @throws DataDelayedException
     */
    private void sortRecords() throws IOException, DataDelayedException {

        if (this.hbaseUtils == null) {
            this.hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX, hbaseConf);
        }

        if (toUpdate.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        Result[] results = hbaseUtils.getAll(getReqs);
        LOG.info("HBase batch get cost: {}ms", System.currentTimeMillis() - start);
        if (results.length != getReqs.size()) {
            LOG.error("HBase batch get expect to return: {}, but actural: {}", getReqs.size(), results.length);
            throw new DataDelayedException("");
        }
        //check can update , and sort records by recordId
        for (int i = 0; i < results.length; i++) {

            try {
                Result result = results[i];
                RecordIdentifier recordIdentifier;
                long recordUpdateTime = bizIdToUpdateTime.get(businessIds.get(i));

                //when there is no insert record or update record before
                if (result.isEmpty()) {
                    if (insertNoExist) {
                        //skip delete when no record exist in HBase
                        if (Operation.DELETE.code().equals(toUpdate.get(i).get("op").toString())) {
                            continue;
                        } else { //update as insert when no record exist in HBase
                            recordIdentifier = new RecordIdentifier(transactionId,
                                    BUCKET_ID, rowId++);
                            LOG.info("treat {} update record as insert: {}", table, businessIds.get(i));
                        }
                    } else {
                        LOG.warn("no recordId in HBase or get empty value for: {}", businessIds.get(i));
                        throw new DataDelayedException("");
                    }
                } else {
                    byte[] idBytes = result.getValue(columnFamily, recordIdColIdentifier);
                    byte[] timeBytes = result.getValue(columnFamily, updateTimeColIdentifier);
                    //recordId= transactionId_BUCKET_ID _rowId
                    String recordId = null == idBytes ? null : Bytes.toString(idBytes);
                    if (null == recordId || recordId.isEmpty()) {
                        LOG.error("get null or empty recordId， bizId: {}", businessIds.get(i));
                        throw new RuntimeException("get null or empty recordId");
                    }
                    Long hbaseTime = null == timeBytes ? null : Bytes.toLong(timeBytes);
                    if (null != hbaseTime && recordUpdateTime <= hbaseTime) {
                        continue;
                    }
                    String[] recordIds = RecordUtils.splitRecordId(recordId, '_');
                    recordIdentifier = new RecordIdentifier(Integer.valueOf(recordIds[0]),
                            Integer.valueOf(recordIds[1]), Integer.valueOf(recordIds[2]));
                }

                recordIdsortedRecord.put(recordIdentifier, toUpdate.get(i));
                //used in AvroObjectInspector
                bizToRecIdMap.put(businessIds.get(i), recordIdentifier);
                recordIdToBuziId.put(recordIdentifier, businessIds.get(i));
                recordIdToUpdateTime.put(recordIdentifier, recordUpdateTime);
            } catch (Exception e) {
                LOG.error("failed to process record: {}", businessIds.get(i));
                throw new RuntimeException(e);
            }

        }
    }

    @Override
    public void beginStreamTransaction(Schema schema, HiveConf hiveConf) throws Exception {
        this.recordSchema = schema;
        this.checkExist = true;
        this.conf = hiveConf;
        super.beginTransaction(schema, hiveConf);
    }

    @Override
    public long commitTransaction() throws Exception {

        if (toUpdate.isEmpty()) {
            super.commitTransaction();
            return 0l;
        }

        sortRecords();

        if (recordIdsortedRecord.isEmpty()) {
            super.commitTransaction();
            return mutateRecords;
        }

        long writeStart = System.currentTimeMillis();

        List<Put> puts = new ArrayList<>();
        for (Map.Entry<RecordIdentifier, GenericData.Record> entry : recordIdsortedRecord.entrySet()) {
            GenericData.Record record = entry.getValue();
            if (record == null) {
                continue;
            }
            if (Operation.DELETE.code().equals(record.get("op").toString())) {
                mutateCoordinator.delete(partitions, record);
            } else {
                //if recordId's transactionId the same as current transaction, means we treat update as insert
                if (insertNoExist && entry.getKey().getTransactionId() == transactionId) {
                    mutateCoordinator.insert(partitions, record);
                } else {
                    mutateCoordinator.update(partitions, record);
                }
            }

            Long recordUpdateTime = recordIdToUpdateTime.get(entry.getKey());
            if (null == latestUpdateTime || this.latestUpdateTime < recordUpdateTime) {
                this.latestUpdateTime = recordUpdateTime;
            }

            Put put = new Put(Bytes.toBytes(recordIdToBuziId.get(entry.getKey())));

            //need to update recordId, for it not exist before
            if (insertNoExist && transactionId == entry.getKey().getTransactionId()) {
                String recId = entry.getKey().getTransactionId() + "_" + BUCKET_ID + "_" + entry.getKey().getRowId();
                put.add(columnFamily, recordIdColIdentifier, Bytes.toBytes(recId));
            }
            put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
            puts.add(put);
        }

        LOG.info("update write finished, cost: {}ms", System.currentTimeMillis() - writeStart);

        long commitStart = System.currentTimeMillis();
        super.commitTransaction();
        LOG.info("commit success cost: {}", System.currentTimeMillis() - commitStart);

        long putStart = System.currentTimeMillis();
        hbaseUtils.batchPut(puts);
        LOG.info("batch put success cost: {}", System.currentTimeMillis() - putStart);

        return mutateRecords;
    }
}
