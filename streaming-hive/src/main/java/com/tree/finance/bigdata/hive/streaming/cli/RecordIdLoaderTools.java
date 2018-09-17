package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.config.Constants;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import com.tree.finance.bigdata.hive.streaming.utils.RecordUtils;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.jdbc.HiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.MYSQL_DB_CONF_FILE;
import static com.tree.finance.bigdata.hive.streaming.config.Constants.MYSQL_DB_PASSWORD;
import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_RECORDID_TBL_SUFFIX;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/6 11:29
 */
public class RecordIdLoaderTools {

    private ExecutorService executor;
    private String db;
    private String table;
    private String columns;
    private List<String> columnList = new ArrayList<>();
    private String types;
    private int cores;

    private List<Integer> primaryKeyIndex;
    private Integer updateTimeIndex;

    private byte[] columnFamily;
    private byte[] recordIdentifier;
    private byte[] updateTimeIdentifier;
    private int totalPartitions = 0;

    private static String CONJECT = "_";
    private Logger LOG = LoggerFactory.getLogger(RecordIdLoaderTools.class);

    private AtomicInteger finishedTasks = new AtomicInteger(0);

    public RecordIdLoaderTools(String db, String table, int cores) {
        this.cores = cores;
        this.db = db;
        this.table = table;
        this.columnFamily = Bytes.toBytes(ConfigFactory.getHbaseColumnFamily());
        this.recordIdentifier = Bytes.toBytes(ConfigFactory.getHbaseRecordIdColumnIdentifier());
        this.updateTimeIdentifier = Bytes.toBytes(ConfigFactory.getHbaseUpdateTimeColumnIdentifier());
    }

    public void load() throws Exception {
        executor = Executors.newFixedThreadPool(cores);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
        IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        prepare(iMetaStoreClient);
        PartitionSpecProxy proxy = iMetaStoreClient.listPartitionSpecs(db, table, Integer.MAX_VALUE);
        Iterator<Partition> iterator = proxy.getPartitionIterator();
        FileSystem fs = FileSystem.get(new Configuration());
        while (iterator.hasNext()) {
            String path = iterator.next().getSd().getLocation();
            if (!fs.exists(new Path(path))) {
                continue;
            }
            totalPartitions++;
            executor.submit(new LoadTask(path));
        }
        iMetaStoreClient.close();
        System.out.println("table: " + table +", total partitions: " + totalPartitions);
        executor.shutdown();
        while (!executor.isTerminated()) {
            LOG.info("wait tasks to be finished ...");
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        System.out.println("finished loading table: " + table);
    }

    private void prepare(IMetaStoreClient iMetaStoreClient) throws Exception {
        List<FieldSchema> fieldSchemas = iMetaStoreClient.getFields(db, table);
        StringBuilder columnsBuilder = new StringBuilder();
        StringBuilder typesBuilder = new StringBuilder();
        for (FieldSchema fieldSchema : fieldSchemas) {
            columnList.add(fieldSchema.getName());
            columnsBuilder.append(fieldSchema.getName()).append(',');
            typesBuilder.append(fieldSchema.getType()).append(',');
        }
        this.columns = columnsBuilder.deleteCharAt(columnsBuilder.length() - 1).toString();
        this.types = typesBuilder.deleteCharAt(typesBuilder.length() - 1).toString();
        LOG.info("columns: {}, types: {}", columns, types);

        //found primaryKey
        Properties properties = new Properties();
        properties.load(CreateTools.class.getResourceAsStream(MYSQL_DB_CONF_FILE));
        String mysqlUrl = properties.getProperty(db);
        String user = properties.getProperty(Constants.MYSQL_DB_USER);
        String password = properties.getProperty(MYSQL_DB_PASSWORD);

        this.primaryKeyIndex = getPrimaryKeyIndexs(mysqlUrl, user, password);
        this.updateTimeIndex = getUpdateTimeIndex();
        if (CollectionUtils.isEmpty(primaryKeyIndex)) {
            throw new RuntimeException("primary key not found for table: " + db + "." + table);
        }
        LOG.info("primary key index: {}, update time col: {}", Arrays.toString(primaryKeyIndex.toArray()), updateTimeIndex);
    }

    private Integer getUpdateTimeIndex() {
        String updateTimeCol = RecordUtils.getCreateTimeCol(db + "." + table, columnList);
        if (StringUtils.isEmpty(updateTimeCol)) {
            throw new RuntimeException("update time column not found");
        }
        return columnList.indexOf(updateTimeCol);
    }

    public List<Integer> getPrimaryKeyIndexs(String mysqlUrl, String user, String password) throws Exception {
        List<Integer> keyIndexes = new ArrayList<>();
        Class.forName(HiveDriver.class.getName());
        //to lower case, and sort by alphabetic order
        TreeSet<String> primaryKeys = new TreeSet<>();
        try (Connection connection = DriverManager.getConnection(mysqlUrl, user, password);
             ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, table)
        ) {
            while (resultSet.next()) {
                primaryKeys.add(resultSet.getString("COLUMN_NAME").toLowerCase());
            }
        }
        for (String pk : primaryKeys) {
            for (int i = 0; i < columnList.size(); i++) {
                if (columnList.get(i).equalsIgnoreCase(pk)) {
                    keyIndexes.add(i);
                }
            }
        }
        return keyIndexes;
    }

    private class LoadTask implements Runnable {
        private String path;

        public LoadTask(String path) {
            this.path = path;
        }

        @Override
        public void run() {

            try {
                Configuration conf = new Configuration();
                conf.set("mapred.input.dir", path);
                conf.set("schema.evolution.columns", columns);
                conf.set("schema.evolution.columns.types", types);
                conf.setInt(BUCKET_COUNT, 1);
                JobConf jobConf = new JobConf(conf);
                OrcInputFormat inputFormat = new OrcInputFormat();
                InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 1);
                AcidInputFormat.Options options = new AcidInputFormat.Options(conf);

                HbaseUtils hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX,
                        ConfigFactory.getHbaseConf());

                for (InputSplit inputSplit : inputSplits) {
                    AcidInputFormat.RowReader<OrcStruct> inner = inputFormat.getReader(inputSplit, options);
                    RecordIdentifier identifier = inner.createKey();
                    OrcStruct value = inner.createValue();
                    StringBuilder idSb = new StringBuilder();
                    StringBuilder busiIdSb = new StringBuilder();
                    while (inner.next(identifier, value)) {
                        //RecordId
                        idSb.delete(0, idSb.length());
                        idSb.append(identifier.getTransactionId()).append(CONJECT)
                                .append(identifier.getBucketId()).append(CONJECT)
                                .append(identifier.getRowId());
                        //businessId
                        busiIdSb.delete(0, busiIdSb.length());
                        for (Integer keyIndex : primaryKeyIndex) {
                            busiIdSb.append(value.getFieldValue(keyIndex)).append(CONJECT);
                        }
                        String rowKey =GenericRowIdUtils.addIdWithHash(busiIdSb.deleteCharAt(busiIdSb.length() - 1).toString());

                        Long updateTime = RecordUtils.getFieldAsTimeMillis(value.getFieldValue(updateTimeIndex));
                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(columnFamily, recordIdentifier, Bytes.toBytes(idSb.toString()));
                        put.addColumn(columnFamily, updateTimeIdentifier, Bytes.toBytes(updateTime));
                        hbaseUtils.insertAsync(put);
                    }
                }
                hbaseUtils.close();
                System.out.println(String.format("%s finished %.2f", table, (finishedTasks.incrementAndGet()* 1.0) / totalPartitions));
            } catch (Exception e) {
                LOG.error("failed to load: {}", path, e);
                System.out.println("ERROR: failed to load: " + path);
            }

        }
    }

}
