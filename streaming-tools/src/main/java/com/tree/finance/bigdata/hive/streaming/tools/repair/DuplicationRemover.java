package com.tree.finance.bigdata.hive.streaming.tools.repair;

import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DuplicationRemover {

    private static Logger LOG = LoggerFactory.getLogger(DuplicationRemover.class);

    private static final String REPAIR_DB = "streaming_repair";

    private String db;

    private List<String> tables;

    private String parFileter;

    private int cores;

    private IMetaStoreClient metaStoreClient;

    public DuplicationRemover(String db, List<String> tables, String parFileter, int cores) throws Exception{
        this.db = db;
        this.tables = tables;
        this.parFileter = parFileter;
        this.cores = cores;
        init();
    }

    public void init() throws Exception{
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
        metaStoreClient = new HiveMetaStoreClient(hiveConf);
        if (CollectionUtils.isEmpty(tables)) {
            this.tables.addAll(metaStoreClient.getAllTables(db));
        }
    }

    private void removeDuplicate() {
        for (String table : tables) {
            try {
                markDuplication(table, parFileter);
            } catch (Exception e) {
                LOG.error("failed to remove duplicate for table: {}", table);
            }
        }
    }

    private void markDuplication(String tableStr, String parFileter) throws Exception{

    }


    public static void main(String[] args) throws Exception {
        DuplicationRemoverParser parser = new DuplicationRemoverParser(args);
        try {
            parser = new DuplicationRemoverParser(args);
            parser.init();
        } catch (Exception e) {
            LOG.error("", e);
            parser.printHelp();
            return;
        }
        new DuplicationRemover(parser.getDb(), parser.getTables(), parser.getOptionPartitionFiler(),
                parser.getCores()).removeDuplicate();
    }

}
