package com.tree.finance.bigdata.hive.streaming.tools;

import com.tree.finance.bigdata.hive.streaming.tools.hbase.HFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/20 23:22
 */
public class Main {


    public static void main(String[] args) {
        try {
            String tableName = "test-loader";
            byte[] family = Bytes.toBytes("f");
            // 配置文件设置
            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.master", "172.18.234.102:60020");
//            conf.set("hbase.zookeeper.quorum", "172.18.234.102");
//            conf.set("hbase.zookeeper.property.clientPort", "2181");

            conf.set("hbase.zookeeper.quorum", "cloudera2:2181,cloudera3:2181,cloudera1:2181");
            conf.set("zookeeper.znode.parent", "/hbase");

            conf.set("hbase.metrics.showTableName", "false");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            String outputdir = "/tmp/streaming-id/" + System.currentTimeMillis();
            Path dir = new Path(outputdir);
            Path familydir = new Path(outputdir, Bytes.toString(family));
            FileSystem fs = familydir.getFileSystem(conf);
            BloomType bloomType = BloomType.NONE;
            Configuration tempConf = new Configuration(conf);
            tempConf.set("hbase.metrics.showTableName", "false");
            tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 1.0f);
            // 实例化HFile的Writer，StoreFile实际上只是HFile的轻量级的封装
            StoreFile.Writer writer = null;
            StoreFile.WriterBuilder wb = new StoreFile.WriterBuilder(tempConf, new CacheConfig(tempConf), fs);
            HFileContext fileContext = new HFileContext();
            writer = wb.withFileContext(fileContext)
                    .withOutputDir(familydir)
                    .withBloomType(bloomType)
                    .withComparator(KeyValue.COMPARATOR)
                    .build();

            KeyValue kv1 = null;
            KeyValue kv2 = null;
            KeyValue kv3 = null;
            KeyValue kv4 = null;
            KeyValue kv5 = null;
            KeyValue kv6 = null;
            KeyValue kv7 = null;
            KeyValue kv8 = null;

            byte[] cn = Bytes.toBytes("cn");
            byte[] dt = Bytes.toBytes("dt");
            byte[] ic = Bytes.toBytes("ic");
            byte[] ifs = Bytes.toBytes("if");
            byte[] ip = Bytes.toBytes("ip");
            byte[] le = Bytes.toBytes("le");
            byte[] mn = Bytes.toBytes("mn");
            byte[] pi = Bytes.toBytes("pi");

            for (int i = 0; i < 100; i++) {
                long current = System.currentTimeMillis();
                // rowkey和列都要按照字典序的方式顺序写入，否则会报错的
                kv2 = new KeyValue(Bytes.toBytes(i), family, dt, current, KeyValue.Type.Put, Bytes.toBytes("6"));
                kv1 = new KeyValue(Bytes.toBytes(i), family, cn, current, KeyValue.Type.Put, Bytes.toBytes("3"));
                kv3 = new KeyValue(Bytes.toBytes(i), family, ic, current, KeyValue.Type.Put, Bytes.toBytes("8"));
                kv4 = new KeyValue(Bytes.toBytes(i), family, ifs, current, KeyValue.Type.Put, Bytes.toBytes("7"));
                kv5 = new KeyValue(Bytes.toBytes(i), family, ip, current, KeyValue.Type.Put, Bytes.toBytes("4"));
                kv6 = new KeyValue(Bytes.toBytes(i), family, le, current, KeyValue.Type.Put, Bytes.toBytes("2"));
                kv7 = new KeyValue(Bytes.toBytes(i), family, mn, current, KeyValue.Type.Put, Bytes.toBytes("5"));
                kv8 = new KeyValue(Bytes.toBytes(i), family, pi, current, KeyValue.Type.Put, Bytes.toBytes("1"));

                writer.append(kv1);
                writer.append(kv2);
                writer.append(kv3);
                writer.append(kv4);
                writer.append(kv5);
                writer.append(kv6);
                writer.append(kv7);
                writer.append(kv8);
            }
            writer.close();

            // 把生成的HFile导入到hbase当中
            HTable table = new HTable(conf, tableName);
            LoadIncrementalHFiles loader;
            try {
                loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(dir, table);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void main2(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cloudera2:2181,cloudera3:2181,cloudera1:2181");
        conf.set("zookeeper.znode.parent", "/hbase");

        Path out = new Path("/tmp/hbase-loader" + System.currentTimeMillis());

        HFileWriter fileWriter = new HFileWriter(out, conf);

        TreeMap<ImmutableBytesWritable, KeyValue> map = new TreeMap<>(ImmutableBytesWritable::compareTo);

        map.put(new ImmutableBytesWritable(Bytes.toBytes("2")), null);
        map.put(new ImmutableBytesWritable(Bytes.toBytes("3")), null);
        map.put(new ImmutableBytesWritable(Bytes.toBytes("1")), null);


        for (Map.Entry<ImmutableBytesWritable, KeyValue> entry : map.entrySet()){
            fileWriter.write(entry.getKey(),
                    new KeyValue(entry.getKey().get(), Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes("value")));
            fileWriter.write(entry.getKey(),
                    new KeyValue(entry.getKey().get(), Bytes.toBytes("f"), Bytes.toBytes("c2"), Bytes.toBytes("value")));
        }

        fileWriter.close(null);

        /*String key = "3";
        KeyValue kv = new KeyValue(Bytes.toBytes(key), Bytes.toBytes("f"), Bytes.toBytes("c1"), Bytes.toBytes(key));
        KeyValue kv2 = new KeyValue(Bytes.toBytes("2"), Bytes.toBytes("f"), Bytes.toBytes("c2"), Bytes.toBytes("2"));


        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(key));
        fileWriter.write(rowkey, kv);
        fileWriter.write(rowkey, kv2);
        fileWriter.close(null);

        TableName tableName = TableName.valueOf("test-loader");

//        HTable hTable = new HTable(conf, "test-loader");
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) conn.getTable(tableName);
        new LoadIncrementalHFiles(conf).doBulkLoad(out, table);*/
    }
}
