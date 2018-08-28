package com.tree.finance.bigdata.hive.streaming.hbase;

import com.tree.finance.bigdata.hive.streaming.utils.hbase.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 11:42
 */
public class TestHbaseUtil {

    final String tableName = "streaming_warehouse_rowId2recId_tbl";

    Configuration config = HBaseConfiguration.create();
    Connection connection;
    String zkStr = "cdh4:2181";

    @Before
    public void before() throws Exception {
        config.set("hbase.zookeeper.quorum", zkStr);
        config.set("zookeeper.znode.parent", "/hbase");
        connection = ConnectionFactory.createConnection(config);
    }

    @Test
    public void testCreateTable() throws Exception {
        HbaseUtils.createTale(tableName);
    }

    @Test
    public void testInsert() throws Exception{
        Table table = connection.getTable(TableName.valueOf("testTbl"));
        byte[] columnFamily = Bytes.toBytes("f");
        byte[] qua1 = Bytes.toBytes("q1");
        byte[] qua2 = Bytes.toBytes("q2");

        long start = System.currentTimeMillis();
        List<Put> puts = new ArrayList<>();
        for (int i=0; i<5000; i++){
            Put put = new Put(Bytes.toBytes(i));
            put.addColumn(columnFamily, qua1, Bytes.toBytes(i));
//            put.addColumn(columnFamily, qua2, Bytes.toBytes(i));
            puts.add(put);
        }
        table.put(puts);
        table.close();
        connection.close();
        long end =System.currentTimeMillis();
        System.out.println("cost:  " + (end - start));
    }
}
