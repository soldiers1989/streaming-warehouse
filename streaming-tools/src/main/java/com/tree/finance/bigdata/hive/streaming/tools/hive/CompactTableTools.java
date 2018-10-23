package com.tree.finance.bigdata.hive.streaming.tools.hive;

import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.List;


public class CompactTableTools {

    private static Logger LOG = LoggerFactory.getLogger(CompactTableTools.class);

    private String db;

    private String[] tables;

    private String parFilter;

    private int compactDeltas;

    private Connection connection;

    public CompactTableTools(String db, String[] tables, String parFilter, int compactDeltas) {
        this.db = db;
        this.tables = tables;
        this.parFilter = parFilter;
        this.compactDeltas = compactDeltas;
    }

    public void init() throws Exception {
        this.connection = DriverManager.getConnection(ConfigHolder.getConfig().getHiveServer2Url(), "hive", "hive");
        try (Statement statement = connection.createStatement()) {
            statement.execute("set mapreduce.map.memory.mb=4096m");
        }
    }

    public void compact() throws Exception {
        HiveMetaStoreClient client = new HiveMetaStoreClient(ConfigHolder.getHiveConf());
        //compact all tables in a db if table not set
        if (tables == null || tables.length == 0) {
            String filter = hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER + " like 'hive'";
            List<String> list = client.listTableNamesByFilter(db, filter, Short.MAX_VALUE);
            tables = list.toArray(new String[list.size()]);
        }
        LOG.info("gonna to compact {} tables", tables.length);
        for (String table : tables) {
            try (Statement statement = connection.createStatement()) {
                String sql = String.format("alter table %s.%s set tblproperties('compactor.mapreduce.map.memory.mb'='4096')", db, table);
                statement.execute(sql);
            }
            PartitionSpecProxy proxy = client.listPartitionSpecsByFilter(db, table, parFilter, Integer.MAX_VALUE);
            PartitionSpecProxy.PartitionIterator iterator = proxy.getPartitionIterator();
            while (iterator.hasNext()) {
                compactPartition(table, iterator.next());
            }
            System.out.println("finished compact table: " + table);
        }
    }

    private void compactPartition(String table, Partition par) {
        try {
            Path path = new Path(par.getSd().getLocation());
            FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(path)) {
                if (fs.listStatus(path).length > compactDeltas) {
                    doCompaction(table, par);
                }
            }
        } catch (Exception e) {
            LOG.error("failed to compact: {}", par.getSd().getLocation(), e);
            throw new RuntimeException("failed to compact");
        }
    }

    private void doCompaction(String table, Partition par) throws Exception {
        System.out.println("start compact partition: " + par.getSd().getLocation());
        try (HiveStatement statement = (HiveStatement) connection.createStatement()) {
            List<String> values = par.getValues();
            StringBuilder sb = new StringBuilder("alter table ").append(db).append(".")
                    .append(table).append(" partition (")
                    .append("p_y=").append(values.get(0)).append(",")
                    .append("p_m=").append(values.get(1)).append(",")
                    .append("p_d=").append(values.get(02)).append(")")
                    .append(" compact 'major' and wait");
            new LogThread(statement).start();
            statement.execute(sb.toString());
        }
        System.out.println("finished compact partition: " + par.getSd().getLocation());
    }

    public class LogThread extends Thread {

        HiveStatement statement;

        public LogThread(HiveStatement statement) {
            super.setDaemon(true);
            this.statement = statement;
        }

        @Override
        public void run() {
            try {
                while (!statement.isClosed() && statement.hasMoreLogs()) {
                    try {
                        for (String log : statement.getQueryLog(true, 100)) {
                            LOG.info(log);
                        }
                        Thread.currentThread().sleep(2000L);
                    } catch (SQLException e) { //防止while里面报错，导致一直退不出循环
                        LOG.error("", e);
                        return;
                    } catch (InterruptedException e) {
                        LOG.warn("interuptted...");
                        return;
                    }
                }
            } catch (SQLException e) {
                LOG.error("", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CompactTableParser parser = null;
        try {
            parser = new CompactTableParser(args);
            parser.init();
        } catch (Exception e) {
            LOG.error("", e);
            parser.printHelp();
        }
        CompactTableTools tools = new CompactTableTools(parser.getDb(), parser.getTables(), parser.getPar()
                , parser.getDeltas());
        tools.init();
        tools.compact();
    }
}
