package com.tree.finance.bigdata.hive.streaming.tools.mutate.configuration;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/12 13:50
 */
public class CheckTimeConfigTools {

    private HbaseUtils hbaseUtils;
    private byte[] family;
    private byte[] checkUpdateTimeCol;

    public CheckTimeConfigTools() throws IOException {
        hbaseUtils = HbaseUtils.getTableInstance(ConfigFactory.getSysConfHbaseTbl()
                , ConfigFactory.getHbaseConf());
        family = Bytes.toBytes(ConfigFactory.getHbaseColumnFamily());
        checkUpdateTimeCol = Bytes.toBytes(ConfigFactory.getCheckUpdateTimeQualifier());
    }

    public void setCheckUpdateTime(String db, String table, String partition, String checkTime) throws Exception{
        //rowKey= {db}.{table}_{partition}
        String rowKey = db + "." +  table;
        if (!StringUtils.isEmpty(partition)){
            rowKey = rowKey + "_" + partition;
        }
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family, checkUpdateTimeCol, Bytes.toBytes(checkTime));
        hbaseUtils.syncPut(put);
    }

    public static void main(String[] args) throws Exception {

        CheckTimeConfigParser parser = null;
        try {
            parser = new CheckTimeConfigParser(args);
            parser.init();
        } catch (Exception e) {
            e.printStackTrace();
            parser.printHelp();
            return;
        }

        if (parser.isHelp()) {
            parser.printHelp();
            return;
        }

        CheckTimeConfigTools tools = new CheckTimeConfigTools();

        if (StringUtils.isEmpty(parser.getTable())) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
            IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            for (String table: iMetaStoreClient.getAllTables(parser.getDb())){
                tools.setCheckUpdateTime(parser.getDb(), table, null, String.valueOf(parser.getTimeMillis()));
            }
        } else {
            if (StringUtils.isEmpty(parser.getPar())) {
                System.out.println("partition is provided, but table not specified !");
                return;
            } else {
                tools.setCheckUpdateTime(parser.getDb(), parser.getTable(), parser.getPar(),
                        String.valueOf(parser.getTimeMillis()));
            }
        }

        tools.hbaseUtils.close();

    }
}
