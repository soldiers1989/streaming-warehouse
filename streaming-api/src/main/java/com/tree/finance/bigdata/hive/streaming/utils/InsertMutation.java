package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.schema.SchemaConstants;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 10:33
 */
public class InsertMutation extends Mutation {

    public InsertMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }

    public void insert(GenericData.Record record) throws Exception {
        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(this.updateCol, record);
        GenericData.Record keyRecord = (GenericData.Record) record.get(SchemaConstants.FIELD_KEY);
        String recordId = transactionId + "_" + BUCKET_ID + "_" + (rowId++);

        //record latest update_time of streaming data
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
    }

}
