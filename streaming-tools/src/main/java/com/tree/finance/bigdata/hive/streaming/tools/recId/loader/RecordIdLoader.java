package com.tree.finance.bigdata.hive.streaming.tools.recId.loader;

import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/6 11:23
 */
public class RecordIdLoader {

    static Logger LOG = LoggerFactory.getLogger(RecordIdLoader.class);

    public static void main(String[] args) throws Exception {

        RecordIdLoaderParser parser = null;
        try {
            parser = new RecordIdLoaderParser(args);
            parser.init();
        } catch (Exception e) {
            LOG.error("", e);
            parser.printHelp();
            return;
        }

        if (StringUtils.isEmpty(parser.getTable())) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
            IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            List<String> tables = iMetaStoreClient.getAllTables(parser.getDb());
            for (String table : tables) {
                try {
                    new BulkIdLoader(parser.getDb(), table, parser.getOptionPartitionFiler(),
                            parser.getCores()).load();
                } catch (Exception e) {
                    System.out.println("ERROR: failed to load table: " + table);
                    e.printStackTrace();
                }
            }
            iMetaStoreClient.close();
        } else {
            new BulkIdLoader(parser.getDb(), parser.getTable(), parser.getOptionPartitionFiler(),
                    parser.getCores()).load();
        }

    }
}
