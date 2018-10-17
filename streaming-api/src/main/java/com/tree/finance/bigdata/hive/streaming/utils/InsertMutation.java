package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.schema.SchemaConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 10:33
 */
public class InsertMutation extends Mutation {

    private static Logger LOG = LoggerFactory.getLogger(InsertMutation.class);
    private List<GenericData.Record> toInsert = new ArrayList<>();
    private List<Put> puts = new ArrayList<>();
    private List<Get> gets = new ArrayList<>();
    private String RECID_PREFIX;

    public InsertMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }


    @Override
    public void beginTransaction(Schema schema, HiveConf conf) throws Exception{
        super.beginTransaction(schema, conf);
        this.RECID_PREFIX = transactionId + "_" + BUCKET_ID + "_";
    }

    public void insert(GenericData.Record record) throws Exception {

        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(this.updateCol, record);
        GenericData.Record keyRecord = (GenericData.Record) record.get(SchemaConstants.FIELD_KEY);
        String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(SchemaConstants.FIELD_KEY).schema());

        if (0 == recordUpdateTime) {
            LOG.warn("record update time is null or zero, bizId: {}", businessId);
        }

        //record latest update_time of streaming repair
        if (latestParUpdateTime == null || latestParUpdateTime < recordUpdateTime) {
            this.latestParUpdateTime = recordUpdateTime;
        }

        if (checkExist) {   //add bizId, Get request to cache, avoid recompution
            this.toInsert.add(record);
            this.gets.add(new Get(Bytes.toBytes(businessId)));
            //if check record in HBase, some record will not be inserted, so recId can't generate now, add rowId to put later
            Put put = new Put(Bytes.toBytes(businessId));
            put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
            puts.add(put);
        } else {
            //insert imediatly if not check record exist in HBase, avoid heap memory consumption
            String recordId = RECID_PREFIX + (rowId++);
            mutateCoordinator.insert(partitions, record);
            super.mutateRecords ++;
            //Batch put into HBase after txn commited
            Put put = new Put(Bytes.toBytes(businessId));
            put.addColumn(columnFamily, recordIdColIdentifier, Bytes.toBytes(recordId));
            put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
            puts.add(put);
        }
    }

    public long commitTransaction() throws Exception {

        if (!txnOpen()) {
            return mutateRecords;
        }

        if (!checkExist) {  //records already write to file before, when we not check record exist in HBase
            long mutateNum = super.commitTransaction();
            hbaseUtils.batchPut(puts);
            return mutateNum;
        } else {
            if (toInsert.isEmpty()){
                return super.commitTransaction();
            }
            long getStart = System.currentTimeMillis();
            Result[] results = hbaseUtils.getAll(gets);
            LOG.info("HBase batch get cost: {}ms", System.currentTimeMillis() - getStart);
            if (results.length != gets.size()) {
                LOG.error("unexpected HBase result size, expect: {}, but return: {}", gets.size(),
                        results.length);
                throw new RuntimeException("unexpected HBase result size");
            }
            List<Put> neededPuts = new ArrayList<>();
            for (int i = 0; i < results.length; i++) {
                Result result = results[i];
                if (result == null || result.isEmpty()) {   //no id in HBase, insert record, and add the generated recordId
                    String recordId = RECID_PREFIX + (rowId++);
                    puts.get(i).addColumn(columnFamily, recordIdColIdentifier, Bytes.toBytes(recordId));
                    mutateCoordinator.insert(partitions, toInsert.get(i));
                    super.mutateRecords ++;
                    neededPuts.add(puts.get(i));
                }
            }
            super.commitTransaction();
            hbaseUtils.batchPut(neededPuts);
        }
        return mutateRecords;
    }

    /**
     * sync insert verion, if use this method, commit transaction just use super.commitTransaction()
     * @param record
     * @throws Exception
     */
    /*public void insert(GenericData.Record record) throws Exception {
        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(this.updateCol, record);
        GenericData.Record keyRecord = (GenericData.Record) record.get(SchemaConstants.FIELD_KEY);
        String recordId = transactionId + "_" + BUCKET_ID + "_" + (rowId++);

        //record latest update_time of streaming repair
        if (latestUpdateTime == null) {
            this.latestUpdateTime = recordUpdateTime;
        } else {
            if (latestUpdateTime < recordUpdateTime) {
                this.latestUpdateTime = recordUpdateTime;
            }
        }
        String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(SchemaConstants.FIELD_KEY).schema());

        if (checkExist) {
            Long hbaseUpdateTime = hbaseUtils.getLong(businessId, columnFamily, updateTimeColIdentifier);
            if (null != hbaseUpdateTime) {
                return;
            }
        }

        Put put = new Put(Bytes.toBytes(businessId));
        put.addColumn(columnFamily, recordIdColIdentifier, Bytes.toBytes(recordId));
        put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));

        hbaseUtils.insertAsync(put);
        mutateCoordinator.insert(partitions, record);
    }*/
}
