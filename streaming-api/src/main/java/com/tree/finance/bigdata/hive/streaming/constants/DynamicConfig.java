package com.tree.finance.bigdata.hive.streaming.constants;

import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/5 20:05
 */
public class DynamicConfig {

    private HbaseUtils hbaseUtils;
    private byte[] fixUpdateTime = Bytes.toBytes(ConfigFactory.getCheckUpdateTimeQualifier());
    private byte[] streamUpdateTime = Bytes.toBytes(ConfigFactory.getStreamUpdateTimeQualifier());
    private byte[] colFamily = Bytes.toBytes(ConfigFactory.getHbaseColumnFamily());

    public DynamicConfig () {
        try {
            this.hbaseUtils = HbaseUtils.getTableInstance(ConfigFactory.getSysConfHbaseTbl(), ConfigFactory.getHbaseConf());
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //update stream_update_time
    /*public void setStreamPartitionUpdateTime (String db, String table, String partitionName, String updateTime) throws IOException {
        String rowKey = assembleRowKey(db, table, partitionName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(colFamily, streamUpdateTime, Bytes.toBytes(updateTime));
        hbaseUtils.syncPut(put);
    }
    public void setStreamTableUpdateTime(String db, String table, String latestUpdateTime) throws IOException{
        String rowKey = db + "." + table;
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(colFamily, streamUpdateTime, Bytes.toBytes(latestUpdateTime));
        hbaseUtils.syncPut(put);
    }*/

    //update stream update_time in hbase, include table update_time, partition update_time
    public void refreshStreamTime (String db, String table, String partitionName, String parUpdateTime, String tblUpdateTime) throws Exception {
        List<Put> puts = new ArrayList<>();

        String parKey = assembleRowKey(db, table, partitionName);
        Put parPut = new Put(Bytes.toBytes(parKey));
        parPut.addColumn(colFamily, streamUpdateTime, Bytes.toBytes(parUpdateTime));
        puts.add(parPut);

        String tblKey = db + "." + table;
        Put tblPut = new Put(Bytes.toBytes(tblKey));
        tblPut.addColumn(colFamily, streamUpdateTime, Bytes.toBytes(tblUpdateTime));
        puts.add(tblPut);

        hbaseUtils.batchPut(puts);
    }


    public Long[] getPartitionUpdateTimes (String db, String table, String partitionName) {
        String rowKey = assembleRowKey(db, table, partitionName);
        return hbaseUtils.getStringsAsLongs(rowKey, colFamily, streamUpdateTime, fixUpdateTime);
    }
    public Long[] getTableUpdateTimes (String db, String table) {
        String rowKey = db + "." + table;
        return hbaseUtils.getStringsAsLongs(rowKey, colFamily, streamUpdateTime, fixUpdateTime);
    }


    private String assembleRowKey(String db, String table, String partitionName) {
        return db + "." + table + "_" + partitionName;
    }
    public HbaseUtils getHbaseUtils() {
        return hbaseUtils;
    }

}
