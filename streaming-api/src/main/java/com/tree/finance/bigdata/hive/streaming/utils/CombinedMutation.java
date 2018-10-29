package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.schema.SchemaConstants;
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
import java.util.TreeSet;

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_RECORDID_TBL_SUFFIX;
import static com.tree.finance.bigdata.schema.SchemaConstants.FIELD_KEY;
import static com.tree.finance.bigdata.schema.SchemaConstants.FIELD_OP;

public class CombinedMutation extends Mutation{

    private static Logger LOG = LoggerFactory.getLogger(CombinedMutation.class);
    private TreeSet<RecordIdentifier> recordIdsorted = new TreeSet<>();
    private Object2ObjectOpenHashMap<RecordIdentifier, String> recordIdToBuziId = new Object2ObjectOpenHashMap();
    private Object2LongOpenHashMap<String> bizIdToUpdateTime = new Object2LongOpenHashMap<>();
    private Object2ObjectOpenHashMap<String, GenericData.Record> bizIdToGeneric = new Object2ObjectOpenHashMap();

    public CombinedMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }

    public void mutate(GenericData.Record record) throws Exception {
        if (record == null) {
            return;
        }
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
    private void filterAndSortRecords() throws IOException {
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
            throw new RuntimeException("");
        }
        //check can update , and sort records by recordId
        for (int i = 0; i < results.length; i++) {
            //can't get bizId from results[i].row(), for it will null, when hbase not exist this recordId
            String bizId = bizIds.get(i);
            try {
                Result result = results[i];
                RecordIdentifier recordIdentifier = getOrCreateRecId(bizId, result);
                if (recordIdentifier == null) {
                    continue;
                }

                if (!bizIdToGeneric.get(bizId).get(FIELD_OP).equals(Operation.CREATE)) {
                    //add update or delete recordIdentifier to map, will be used in AvroObjectInspector for update and delete
                    bizToRecIdMap.put(bizId, recordIdentifier);
                }

                recordIdsorted.add(recordIdentifier);
                recordIdToBuziId.put(recordIdentifier, bizId);
            }  catch (Exception e) {
                LOG.error("failed to process record: {}", bizId);
                throw new RuntimeException(e);
            }
        }
    }

    private RecordIdentifier getOrCreateRecId(String bizId, Result result) {
        RecordIdentifier recordIdentifier;
        long recordUpdateTime = bizIdToUpdateTime.get(bizId);
        //when there is no insert record or update record before
        if (result.isEmpty()) {
            //for delete, it's safe to delete when record havn't insert yet, and will update HBase later
            //for update and insert operation, insert record when no recordId exist in HBase
            recordIdentifier = new RecordIdentifier(transactionId,
                    BUCKET_ID, rowId++);
        } else {    //if recId exist in HBase
            //skip insert
            if (Operation.CREATE.code().equals(bizIdToGeneric.get(bizId).get("op").toString())) {
                return null;
            } else {    //update or delete
                byte[] idBytes = result.getValue(columnFamily, recordIdColIdentifier);
                byte[] timeBytes = result.getValue(columnFamily, updateTimeColIdentifier);
                //recordId= transactionId_BUCKET_ID _rowId
                String recordId = null == idBytes ? null : Bytes.toString(idBytes);
                if (null == recordId || recordId.isEmpty()) {
                    LOG.error("key exists, but get null or empty recordId，may cause data inacurracy bizId: {}", bizId);
                    throw new RuntimeException("key exists, but get null or empty recordId");
                }
                Long hbaseTime = null == timeBytes ? null : Bytes.toLong(timeBytes);
                //for delete operation, record update_time is the same as last operation.
                if (null != hbaseTime && recordUpdateTime < hbaseTime) {
                    return null;
                }
                String[] recordIds = RecordUtils.splitRecordId(recordId, '_');
                recordIdentifier = new RecordIdentifier(Integer.valueOf(recordIds[0]),
                        Integer.valueOf(recordIds[1]), Integer.valueOf(recordIds[2]));
            }
        }
        return recordIdentifier;
    }

    @Override
    public long commitTransaction() throws Exception {
        if (bizIdToGeneric.isEmpty()) {
            super.commitTransaction();
            return 0l;
        }
        filterAndSortRecords();
        if (recordIdsorted.isEmpty()) {
            super.commitTransaction();
            return mutateRecords;
        }
        doMutate();
        return mutateRecords;
    }

    private void doMutate() throws Exception {
        long writeStart = System.currentTimeMillis();

        List<Put> puts = new ArrayList<>();
        for (RecordIdentifier recordIdentifier : recordIdsorted) {
            try {
                GenericData.Record record = bizIdToGeneric.get(recordIdToBuziId.get(recordIdentifier));

                if (Operation.UPDATE.code().equals(record.get(FIELD_OP).toString())) {
                    //if recordId's transactionId the same as current transaction, means we treat update as insert
                    if (recordIdentifier.getTransactionId() == transactionId) {
                        mutateCoordinator.insert(partitions, record);
                    } else {
                        mutateCoordinator.update(partitions, record);
                    }
                } else if (Operation.CREATE.code().equals(record.get(FIELD_OP).toString())) {
                    mutateCoordinator.insert(partitions, record);
                } else if (Operation.DELETE.code().equals(record.get(FIELD_OP).toString())) {  //have filtered non exist delete already, and safe to multi delete
                    mutateCoordinator.delete(partitions, record);
                } else {
                    LOG.error("unsupported operation: {}", record.get(FIELD_OP));
                    throw new RuntimeException("unsupported operation: " + FIELD_OP);
                }

                //update stream update time
                Long recordUpdateTime = bizIdToUpdateTime.get(recordIdToBuziId.get(recordIdentifier));
                if (null == latestParUpdateTime || this.latestParUpdateTime < recordUpdateTime) {
                    this.latestParUpdateTime = recordUpdateTime;
                }

                Put put = new Put(Bytes.toBytes(recordIdToBuziId.get(recordIdentifier)));
                //need to update recordId, for it not exist before
                if (transactionId == recordIdentifier.getTransactionId()) {
                    String recId = recordIdentifier.getTransactionId() + "_" + BUCKET_ID + "_" + recordIdentifier.getRowId();
                    put.add(columnFamily, recordIdColIdentifier, Bytes.toBytes(recId));
                }
                put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
                puts.add(put);
            }catch (Exception e) {
                LOG.error("failed to mutate record, id: {}, record: {}",
                        recordIdToBuziId.get(recordIdentifier), bizIdToGeneric.get(recordIdToBuziId.get(recordIdentifier)));
                throw e;
            }
        }
        LOG.info("mutate write finished, cost: {}ms", System.currentTimeMillis() - writeStart);
        long commitStart = System.currentTimeMillis();
        super.commitTransaction();
        LOG.info("commit success cost: {}", System.currentTimeMillis() - commitStart);
        long putStart = System.currentTimeMillis();
        hbaseUtils.batchPut(puts);
        LOG.info("batch put ids success cost: {}", System.currentTimeMillis() - putStart);
    }

}
