package com.tree.finance.bigdata.hive.streaming.hbase;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
    String zkStr = "cloudera2:2181";


    @Test
    public void testCreateTable() throws Exception {
        HbaseUtils.createTale(tableName, "f", 0);
    }

    @Test
    public void testBatchGet()throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cloudera2:2181,cloudera3:2181,cloudera1:2181");
        HbaseUtils hbaseUtils = HbaseUtils.getTableInstance("operator.t_tel_call_sheet_id", conf);
        List<Get> gets = new ArrayList<>();
        gets.add(new Get(Bytes.toBytes("2222222")));
        gets.add(new Get(Bytes.toBytes("22222222333")));
        Result resuts[] = hbaseUtils.getAll(gets);
        for (Result result : resuts) {
            if (result.isEmpty()) {
                System.out.println(Bytes.toString(result.value()));
            }

        }
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

    @Test
    public void TestRegionLocator() throws Exception{
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cloudera2:2181,cloudera3:2181,cloudera1:2181");
        connection = ConnectionFactory.createConnection(conf);
        Pair<byte[][], byte[][]> startEndKeys = connection.getRegionLocator(TableName.valueOf("point.pc_user_ip_daily_log_id")).getStartEndKeys();

        byte[][] first = startEndKeys.getFirst();
        byte[][] second = startEndKeys.getSecond();
        System.out.println("pair key length " + second.length);
        for (int i=0; i < first.length; i++) {
            System.out.println(Bytes.toString(first[i]) + " | " + Bytes.toString(second[i]));
        }
        connection.close();
        byte[]  a = {1, 2, 5, 7};
        System.out.println("index " + Arrays.binarySearch(a, (byte) 3));
    }
}
