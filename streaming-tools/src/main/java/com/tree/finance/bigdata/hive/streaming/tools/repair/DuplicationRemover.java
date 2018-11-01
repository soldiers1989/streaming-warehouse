package com.tree.finance.bigdata.hive.streaming.tools.repair;

import com.tree.finance.bigdata.hive.streaming.tools.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.tools.recId.loader.BulkIdLoader;
import com.tree.finance.bigdata.hive.streaming.utils.RecordUtils;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DuplicationRemover {

    private static Logger LOG = LoggerFactory.getLogger(DuplicationRemover.class);

    private static final String REPAIR_DB = "streaming_repair";

    private String db;

    private List<String> tables;

    private String parFileter;

    private int cores;

    private String hive2Url;

    private String metastoreUri;

    private IMetaStoreClient metaStoreClient;

    private Connection connection;

    private boolean executedBefore;

    private static String TEMP_REPAIR_DB = "temp_repair";

    private static String SUCCESS_SUFFIX = "_success";

    public DuplicationRemover(String db, List<String> tables, String parFileter, int cores, String hive2Url, String metastoreUri,
                              boolean executedBefore) throws Exception {
        this.hive2Url = hive2Url;
        this.db = db;
        this.tables = tables;
        this.parFileter = parFileter;
        this.cores = cores;
        this.executedBefore = executedBefore;
        init();
    }

    public void init() throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
        metaStoreClient = new HiveMetaStoreClient(hiveConf);
        if (CollectionUtils.isEmpty(tables)) {
            this.tables.addAll(metaStoreClient.getAllTables(db));
        }
        Properties prop = new Properties();
        prop.setProperty("mapreduce.map.memory.mb", "2048");
        prop.setProperty("hive.exec.dynamic.partition.mode", "nostrict");
        prop.setProperty("hive.exec.max.dynamic.partitions", "99999");
        prop.setProperty("hive.exec.max.dynamic.partitions.pernode", "9999");
        this.connection = DriverManager.getConnection(hive2Url, prop);
    }

    public void removeDuplicate() throws Exception {
        for (String table : tables) {
            try {
                List<FieldSchema> fields = metaStoreClient.getTable(db, table).getSd().getCols();
                List<String> colNames = new ArrayList<>();
                fields.forEach(f -> colNames.add(f.getName().toLowerCase()));

                List<Partition> duplicatePartitions = markDuplication(table, parFileter);

                if (duplicatePartitions.isEmpty()) {
                    System.out.println("no duplication exist in table: " + db + "." + table + " where " + parFileter);
                    if (!executedBefore) {
                        return;
                    }
                }

                extractUniqueData(table, duplicatePartitions, colNames);

                validateDataAccuracy(table, duplicatePartitions);

                dropDuplicatePartition(table, duplicatePartitions);

                colNames.add("p_y");
                colNames.add("p_m");
                colNames.add("p_d");
                if (insertUniqData(table, colNames)){
                    System.out.println("start to load id to HBase");

                    UserGroupInformation ugi =
                            UserGroupInformation.createRemoteUser("hbase");
                    ugi.doAs(new PrivilegedExceptionAction<Void>() {
                        public Void run() throws Exception {
                            new BulkIdLoader(db, table, buildParConditions(duplicatePartitions), cores, null).load();
                            return null;
                        }
                    });
                    System.out.println("finished load id to HBase");
                    System.out.println("start to validate repair");
                    validateRepair(table, duplicatePartitions);
                    System.out.println("finished validate repair");
                    dropTempTable(table);
                }

                LOG.info("finished remove duplicate for table : {}", db + "." + table);

            } catch (Exception e) {
                LOG.error("failed to remove duplicate for table: {}", table, e);
                throw e;
            }
        }
    }

    private void dropTempTable(String table) throws Exception{
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists " + TEMP_REPAIR_DB + "." + table + SUCCESS_SUFFIX);
        }
    }

    private boolean validateRepair(String table, List<Partition> duplicatePartitions) throws Exception {

        String parConditions;
        if (CollectionUtils.isEmpty(duplicatePartitions)) {
            parConditions = parFileter;
        }else {
            parConditions = buildParConditions(duplicatePartitions);
        }

        StringBuilder standSql = new StringBuilder("select count(id), count(distinct id) from ")
                .append(db).append('.').append(table).append(" where ").append(parConditions);

        try (Statement statement = connection.createStatement()) {

            ResultSet standSet = statement.executeQuery(standSql.toString());
            if (!standSet.next()) {
                throw new RuntimeException("illega state");
            }
            long count = standSet.getLong(1);
            long distinctCount = standSet.getLong(2);
            standSet.close();

            if (count != distinctCount) {
                LOG.error("data still not accurate after repair");
                throw new RuntimeException("data inaccuracy after repair");
            }
            return true;
        }
    }

    private boolean insertUniqData(String table, List<String> colNames) throws Exception {

        String quanlifiledTbl = db + "." + table;
        StringBuilder sb = new StringBuilder("insert into table ")
                .append(quanlifiledTbl).append(" partition(p_y,p_m,p_d) select ")
                .append(concatColNames(colNames))
                .append(" from ").append(TEMP_REPAIR_DB).append('.').append(table).append(SUCCESS_SUFFIX)
                .append(" where rowno = 1");
        try (Statement statement = connection.createStatement()) {

            ResultSet tableExist = connection.getMetaData().getTables(null, TEMP_REPAIR_DB, table + SUCCESS_SUFFIX, null);
            if (tableExist.next()) {
                StringBuilder historySb = new StringBuilder("select count(id) from ")
                        .append(TEMP_REPAIR_DB).append('.').append(table).append(SUCCESS_SUFFIX).append(" where rowno = 1");
                ResultSet historyCountSet = statement.executeQuery(historySb.toString());
                if (!historyCountSet.next()) {
                    System.out.println("no data to insert");
                    historyCountSet.close();
                    return false;
                }
            }

            statement.execute("set hive.exec.dynamic.partition.mode=nonstrict");
            statement.execute("set hive.exec.max.dynamic.partitions=99999");
            statement.execute("set hive.exec.max.dynamic.partitions.pernode=99999");
            System.out.println("start insert unique data");
            statement.execute(sb.toString());
            System.out.println("finished insert unique data");
            return true;
        }
    }

    public String concatColNames(List<String> colNames) {
        StringBuilder sb = new StringBuilder();
        for (String clo : colNames) {
            sb.append(clo).append(',');
        }
        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    private void dropDuplicatePartition(String table, List<Partition> duplicatePartitions) throws Exception {
        String qualifiedTbl = db + "." + table;
        try (Statement statement = connection.createStatement()) {
            for (Partition par : duplicatePartitions) {
                String sql = "alter table " + qualifiedTbl + " drop if exists partition " + par.buildDropParSpec();
                LOG.info("gong to drop partition, execte: {}", sql);
                statement.execute(sql);
            }
        }
    }


    private boolean validateDataAccuracy(String table, List<Partition> duplicatePartitions) throws Exception {

        if (CollectionUtils.isEmpty(duplicatePartitions)) {
            return true;
        }

        try (Statement statement = connection.createStatement()) {

            long historyCount = 0;
            ResultSet tableExist = connection.getMetaData().getTables(null, TEMP_REPAIR_DB, table + SUCCESS_SUFFIX, null);
            if (tableExist.next()) {
                StringBuilder historySb = new StringBuilder("select count(id) from ")
                        .append(TEMP_REPAIR_DB).append('.').append(table).append(SUCCESS_SUFFIX).append(" where rowno = 1");
                ResultSet historyCountSet = statement.executeQuery(historySb.toString());
                if (historyCountSet.next()) {
                    historyCount = historyCountSet.getLong(1);
                }
                historyCountSet.close();
            }

            StringBuilder sb = new StringBuilder();
            String parConditions = buildParConditions(duplicatePartitions);

            StringBuilder standSql = new StringBuilder("select count(distinct id) from ")
                    .append(db).append('.').append(table).append(" where ").append(parConditions);
            StringBuilder toVerify = new StringBuilder("select count(id) from ")
                    .append(TEMP_REPAIR_DB).append('.').append(table).append(" where rowno = 1");

            System.out.println("start to check data consistancy.");
            ResultSet standSet = statement.executeQuery(standSql.toString());
            if (!standSet.next()) {
                throw new RuntimeException("illega state");
            }
            long standCount = standSet.getLong(1);
            standSet.close();

            // maybe we have drop table partition, in last failed insert unique data attempt;
            if (historyCount > standCount) {
                LOG.info("history table got more data, use history table");
                System.out.println("going to use history version of unique data: " + TEMP_REPAIR_DB + "." + table);
                return true;
            } else {
                ResultSet set = statement.executeQuery(toVerify.toString());
                if (!set.next()) {
                    throw new RuntimeException("illega state");
                }
                long extractCount = set.getLong(1);
                set.close();

                if (standCount != extractCount) {
                    LOG.warn("data inacurracy, standarized count: {}, extract count: {}", standCount, extractCount);
                    throw new RuntimeException("data inaccuracy.");
                } else {
                    statement.execute("drop table if exists " + TEMP_REPAIR_DB + "." + table + SUCCESS_SUFFIX);
                    String sql = String.format("ALTER TABLE %s RENAME TO %s ", TEMP_REPAIR_DB + "." + table,
                            TEMP_REPAIR_DB + "." + table + SUCCESS_SUFFIX);
                    LOG.info("going to rename table, execute: {}", sql);
                    statement.execute(sql);
                    System.out.println("saved consistent unique data");
                    LOG.info("finished rename table");
                }
                System.out.println(String.format("data in consistancy standrize: %d extracted: %d", standCount, extractCount));
            }
            return true;
        }
    }

    private String buildParConditions(List<Partition> duplicatePartitions) {

        if (CollectionUtils.isEmpty(duplicatePartitions)) {
            return parFileter;
        }

        StringBuilder parQueryConditions = new StringBuilder();
        for (int i = 0; i < duplicatePartitions.size(); i++) {
            String parCondition = duplicatePartitions.get(i).buildQueryCondition();
            if (i != 0) {
                parQueryConditions.append(" or ").append(parCondition);
            } else {
                parQueryConditions.append(" ").append(parCondition);
            }
        }
        return parQueryConditions.toString();
    }

    private boolean extractUniqueData(String tableStr, List<Partition> duplicatePartitions, List<String> colNames) throws Exception {

        if (duplicatePartitions.isEmpty()) {
            return true;
        }

        String updateCol = RecordUtils.getUpdateCol(tableStr, colNames);
        try (Statement statement = connection.createStatement()) {
            statement.execute("create schema if not exists " + TEMP_REPAIR_DB);
            //not drop table.
            statement.execute("drop table if exists " + TEMP_REPAIR_DB + "." + tableStr);
            StringBuilder getDistinctData = new StringBuilder("create table ")
                    .append(TEMP_REPAIR_DB).append(".").append(tableStr)
                    .append(" STORED AS orc as select *, row_number()  over (partition by id order by ")
                    .append(updateCol)
                    .append(" desc) as rowno from ").append(db).append(".").append(tableStr)
                    .append(" where id is not null and ").append(buildParConditions(duplicatePartitions));
            LOG.info("going to create unique data, execute: {}", getDistinctData);
            System.out.println("start to extract unique data...");
            statement.execute(getDistinctData.toString());
            System.out.println("finished extract unique data...");
            return true;
        }
    }

    private List<Partition> markDuplication(String tableStr, String parFileter) throws Exception {
        String quanlifiedTbl = db + "." + tableStr;
        List<Partition> duplicatePartitions = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            StringBuilder duplicateSql = new StringBuilder("select p_y, p_m, p_d, count(distinct id), count(id) from ")
                    .append(quanlifiedTbl);
            if (!StringUtils.isEmpty(parFileter)) {
                duplicateSql.append(" where ").append(parFileter);
            }
            duplicateSql.append(" group by p_y, p_m, p_d having count(distinct id) != count(id)")
                    .append(" order by p_y, p_m, p_d asc");
            LOG.info("going to find duplicate partition, execute: {}", duplicateSql);
            System.out.println("start marking duplicate partitions...");
            ResultSet resultSet = statement.executeQuery(duplicateSql.toString());
            System.out.println("--------------------- duplicate partitions -------------------------");
            System.out.println(String.format("| %-9s  |  %-10s  |  %-15s  |  %s |", "partition", "count", "distinctCount", "No"));
            while (resultSet.next()) {
                int y = resultSet.getInt(1);
                int m = resultSet.getInt(2);
                int d = resultSet.getInt(3);
                long distinctId = resultSet.getLong(4);
                long countId = resultSet.getLong(5);
                duplicatePartitions.add(new Partition(y, m, d));
                System.out.println(String.format("| %-9s  |  %-10s  |  %-15s  |  %s |",
                        y + "-" + m + "-" + d, countId, distinctId, countId - distinctId));
            }
            System.out.println("--------------------- duplicate partitions -------------------------");
        }
        return duplicatePartitions;
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
        AppConfig config = ConfigHolder.getConfig();
        new DuplicationRemover(parser.getDb(), parser.getTables(), parser.getOptionPartitionFiler(),
                parser.getCores(), config.getHiveServer2Url(), config.getMetastoreUris(), parser.getExecuted()).removeDuplicate();
    }

    class Partition {
        private int y;
        private int m;
        private int d;

        Partition(int y, int m, int d) {
            this.y = y;
            this.m = m;
            this.d = d;
        }

        public int getY() {
            return y;
        }

        public int getM() {
            return m;
        }

        public int getD() {
            return d;
        }

        public String buildQueryCondition() {
            return "(p_y=" + y + " and p_m=" + m + " and p_d=" + d + ")";
        }

        private String buildDropParSpec() {
            return "(p_y=" + y + " , p_m=" + m + " , p_d=" + d + ")";
        }
    }

}
