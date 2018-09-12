package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.utils.record.RecordIdLoaderTools;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
        }

        if (StringUtils.isEmpty(parser.getTable())) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
            IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            List<String> tables = iMetaStoreClient.getAllTables(parser.getDb());
            for (String table : tables) {
                new RecordIdLoaderTools(parser.getDb(), table, parser.getCores()).load();
            }
            iMetaStoreClient.close();
        }else {
            new RecordIdLoaderTools(parser.getDb(), parser.getTable(), parser.getCores()).load();
        }

    }
}
