package com.tree.finance.bigdata.hive.streaming.tools;

import com.tree.finance.bigdata.hive.streaming.tools.hbase.HFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/20 23:22
 */
public class Main {
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cloudera2:2181,cloudera3:2181,cloudera1:2181");
        conf.set("zookeeper.znode.parent", "/hbase");

        Path out = new Path("/tmp/hbase-loader" + System.currentTimeMillis());

        HFileWriter fileWriter = new HFileWriter(out, conf);
        String key = "1";
        KeyValue kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes(key));
        KeyValue kv2 = new KeyValue(Bytes.toBytes("2"), Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("2"));


        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(key));
        fileWriter.write(rowkey, kv);
        fileWriter.write(rowkey, kv2);
        fileWriter.close(null);

        TableName tableName = TableName.valueOf("test-loader");

//        HTable hTable = new HTable(conf, "test-loader");
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) conn.getTable(tableName);
        new LoadIncrementalHFiles(conf).doBulkLoad(out, table);
    }
}
