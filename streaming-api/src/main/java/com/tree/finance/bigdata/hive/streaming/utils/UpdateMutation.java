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
import java.util.*;

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

    TreeSet<RecordIdentifier> recordIdsorted = new TreeSet<>();
    Object2ObjectOpenHashMap<RecordIdentifier, String> recordIdToBuziId = new Object2ObjectOpenHashMap();

    Object2LongOpenHashMap<String> bizIdToUpdateTime = new Object2LongOpenHashMap<>();
    Object2ObjectOpenHashMap<String, GenericData.Record> bizIdToGeneric = new Object2ObjectOpenHashMap();

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
        } else { //newer or not exist record
            bizIdToUpdateTime.put(businessId, recordUpdateTime);
            bizIdToGeneric.put(businessId, record);
        }
    }

    /**
     * @throws IOException
     * @throws DataDelayedException
     */
    private void sortRecords() throws IOException, DataDelayedException {

        if (this.hbaseUtils == null) {
            this.hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX, hbaseConf);
        }

        //keep HBase gets and buzId in coresponding order, for Result will not return requested rowkey when result is empty
        List<Get> getReqs = new ArrayList<>();
        List<String> bizIds = new ArrayList<>();
        for (String key : bizIdToGeneric.keySet()){
            getReqs.add(new Get(Bytes.toBytes(key)));
            bizIds.add(key);
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
            //can't get bizId from results[i].row(), for it will be null, when hbase not exist this recordId
            String bizId = bizIds.get(i);
            try {
                Result result = results[i];
                RecordIdentifier recordIdentifier;
                long recordUpdateTime = bizIdToUpdateTime.get(bizId);

                //when there is no insert record or update record before
                if (result.isEmpty()) {
                    if (insertNoExist) {
                        //skip delete when no record exist in HBase
                        if (Operation.DELETE.code().equals(bizIdToGeneric.get(bizId).get("op").toString())) {
                            continue;
                        } else { //update as insert when no record exist in HBase
                            recordIdentifier = new RecordIdentifier(transactionId,
                                    BUCKET_ID, rowId++);
                        }
                    } else {
                        LOG.warn("no recordId in HBase or get empty value for: {}", bizId);
                        throw new DataDelayedException("");
                    }
                } else {
                    byte[] idBytes = result.getValue(columnFamily, recordIdColIdentifier);
                    byte[] timeBytes = result.getValue(columnFamily, updateTimeColIdentifier);
                    //recordId= transactionId_BUCKET_ID _rowId
                    String recordId = null == idBytes ? null : Bytes.toString(idBytes);
                    if (null == recordId || recordId.isEmpty()) {
                        LOG.error("get null or empty recordId， bizId: {}", bizId);
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

                recordIdsorted.add(recordIdentifier);
                //used in AvroObjectInspector
                bizToRecIdMap.put(bizId, recordIdentifier);
                recordIdToBuziId.put(recordIdentifier, bizId);
            } catch (DataDelayedException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("failed to process record: {}", bizId);
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

        if (bizIdToGeneric.isEmpty()) {
            super.commitTransaction();
            return 0l;
        }

        sortRecords();

        if (recordIdsorted.isEmpty()) {
            super.commitTransaction();
            return mutateRecords;
        }

        long writeStart = System.currentTimeMillis();

        List<Put> puts = new ArrayList<>();
        for (RecordIdentifier recordIdentifier : recordIdsorted) {
            GenericData.Record record = bizIdToGeneric.get(recordIdToBuziId.get(recordIdentifier));
            if (record == null) {
                continue;
            }
            if (Operation.DELETE.code().equals(record.get("op").toString())) {  //have filtered non exist delete before
                mutateCoordinator.delete(partitions, record);
            } else {
                //if recordId's transactionId the same as current transaction, means we treat update as insert
                if (insertNoExist && recordIdentifier.getTransactionId() == transactionId) {
                    mutateCoordinator.insert(partitions, record);
                } else {
                    mutateCoordinator.update(partitions, record);
                }
            }

            Long recordUpdateTime = bizIdToUpdateTime.get(recordIdToBuziId.get(recordIdentifier));

            if (null == latestUpdateTime || this.latestUpdateTime < recordUpdateTime) {
                this.latestUpdateTime = recordUpdateTime;
            }

            Put put = new Put(Bytes.toBytes(recordIdToBuziId.get(recordIdentifier)));

            //need to update recordId, for it not exist before
            if (insertNoExist && transactionId == recordIdentifier.getTransactionId()) {
                String recId = recordIdentifier.getTransactionId() + "_" + BUCKET_ID + "_" + recordIdentifier.getRowId();
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
        LOG.info("batch put ids success cost: {}", System.currentTimeMillis() - putStart);

        return mutateRecords;
    }
}
