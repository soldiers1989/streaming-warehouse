package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.schema.SchemaConstants;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 10:33
 */
public class InsertMutation extends Mutation{

    public InsertMutation(String db, String table, String partition, List<String> partitions, String metastoreUris, Configuration hbaseConf) {
        super(db, table, partition, partitions, metastoreUris, hbaseConf);
    }

    public void insert(GenericData.Record record, boolean checkExist) throws Exception {
        Long recordUpdateTime = RecordUtils.getFieldAsTimeMillis(this.updateCol, record);
        GenericData.Record keyRecord = (GenericData.Record) record.get(SchemaConstants.FIELD_KEY);
        String recordId = transactionId + "_" + BUCKET_ID + "_" + (rowId++);

        if (checkExist) {
            Long hbaseUpdateTime = hbaseUtils.getLong(recordId, columnFamily, updateTimeColIdentifier);
            if (null != hbaseUpdateTime && hbaseUpdateTime >= recordUpdateTime) {
                return;
            }
        }
        Put put = new Put(Bytes.toBytes(dbTblPrefix +
                GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(SchemaConstants.FIELD_KEY).schema()))
        );
        put.addColumn(columnFamily, recordIdColIdentifier, Bytes.toBytes(recordId));
        if (null != recordUpdateTime) {
            put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(recordUpdateTime));
        }
        hbaseUtils.insertAsync(put);
        mutateCoordinator.insert(partitions, record);
    }

}
