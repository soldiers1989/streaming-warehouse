package com.tree.finance.bigdata.hive.streaming.tools.hbase.mapreduce;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.tools.config.Constants;
import com.tree.finance.bigdata.hive.streaming.tools.hbase.RecordIdLoaderTools;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import com.tree.finance.bigdata.hive.streaming.utils.RecordUtils;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/21 15:02
 */
public class SparkIdLoader implements Serializable {

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

    private long cacheRecords;

    private AtomicInteger finishedTasks = new AtomicInteger(0);

    SparkConf sparkConf;


    public SparkIdLoader(String db, String table, int cores, long cacheRecords) {
        this.cacheRecords = cacheRecords;
        this.cores = cores;
        this.db = db;
        this.table = table;
        this.columnFamily = Bytes.toBytes(ConfigFactory.getHbaseColumnFamily());
        this.recordIdentifier = Bytes.toBytes(ConfigFactory.getHbaseRecordIdColumnIdentifier());
        this.updateTimeIdentifier = Bytes.toBytes(ConfigFactory.getHbaseUpdateTimeColumnIdentifier());
        sparkConf = new SparkConf();
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hbase");

        int cores = Integer.valueOf(args[0]);
        String db = args[1];
        long cacheRecords = Long.valueOf(args[2]);
        String tableSplits[] = args.length >= 4 ? args[3].split(",") : null;

        JavaSparkContext sparkContext = new JavaSparkContext();

        if (null == tableSplits || tableSplits.length == 0) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
            IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            List<String> list = iMetaStoreClient.getAllTables(db);
            tableSplits = list.toArray(new String[list.size()]);
            iMetaStoreClient.close();
        }
        for (String t : tableSplits) {
            try {
                new SparkIdLoader(db, t, cores, cacheRecords).load(sparkContext);
            } catch (Exception e) {
                System.out.println("ERROR: failed to load table: " + t);
                throw e;
            }

        }

    }


    private class KVComparator implements Comparator<Cell> {
        @Override
        public int compare(Cell left, Cell right) {
            int compare = CellComparator.compare(left, right, false);
            return compare;
        }
    }

    private class LoadFunction implements VoidFunction<Iterator<String>>, Serializable {

        @Override
        public void call(Iterator<String> stringIterator) throws Exception {
            long wroteRecords = 0;
            TreeSet<KeyValue> kvSets = new TreeSet<>(new KVComparator());
            StoreFile.Writer writer = null;
            Path outPut = null;
            while (stringIterator.hasNext()) {
                Path path = new Path(stringIterator.next());
                Configuration conf = new Configuration();
                conf.set("mapred.input.dir", path.toString());
                conf.set("schema.evolution.columns", columns);
                conf.set("schema.evolution.columns.types", types);
                conf.setInt(hive_metastoreConstants.BUCKET_COUNT, 1);
                if (writer == null) {
                    FileSystem fs = FileSystem.get(new Configuration());
                    outPut = new Path("/tmp/streaming-hbase/" + db + "/" + table + "/" + UUID.randomUUID().getLeastSignificantBits());
                    Integer mb = Integer.valueOf(sparkConf.get("table_size"));
                    Configuration hbaseConf = ConfigFactory.getHbaseConf();
                    if (mb > 100 && mb < 500) {
                        hbaseConf.setInt(HbaseUtils.PRE_SPLIT_REGIONS, 3);
                    } else if (mb > 500 && mb < 1000) {
                        hbaseConf.setInt(HbaseUtils.PRE_SPLIT_REGIONS, 5);
                    } else if (mb > 1000) {
                        hbaseConf.setInt(HbaseUtils.PRE_SPLIT_REGIONS, 7);
                    }
                    if (!fs.exists(outPut)) {
                        fs.mkdirs(outPut);
                    }
                    StoreFile.WriterBuilder wb = new StoreFile.WriterBuilder(conf, new CacheConfig(hbaseConf), fs);
                    HFileContext fileContext = new HFileContext();
                    writer = wb.withFileContext(fileContext)
                            .withOutputDir(new Path(outPut, Bytes.toString(columnFamily)))
                            .withBloomType(BloomType.NONE)
                            .withComparator(KeyValue.COMPARATOR)
                            .build();
                }

                JobConf jobConf = new JobConf(conf);
                OrcInputFormat inputFormat = new OrcInputFormat();
                InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 1);
                AcidInputFormat.Options options = new AcidInputFormat.Options(conf);

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
                        String rowKey = GenericRowIdUtils.addIdWithHash(busiIdSb.deleteCharAt(busiIdSb.length() - 1).toString());
                        Long updateTime = RecordUtils.getFieldAsTimeMillis(value.getFieldValue(updateTimeIndex));

                        if (null == updateTime) {
                            //set update_time to 0
                            updateTime = 0L;
                        }

                        long current = System.currentTimeMillis();
                        byte[] key = Bytes.toBytes(rowKey);
                        kvSets.add(new KeyValue(key, columnFamily, recordIdentifier, current,
                                KeyValue.Type.Put, Bytes.toBytes(idSb.toString())));
                        kvSets.add(new KeyValue(key, columnFamily, updateTimeIdentifier, current,
                                KeyValue.Type.Put, Bytes.toBytes(updateTime)));
                        wroteRecords++;
                    }
                    inner.close();
                }

                if (wroteRecords >= cacheRecords) {

                    System.out.println("ready to flush");

                    for (KeyValue kv : kvSets) {
                        writer.append(kv);
                    }

                    writer.close();
                    wroteRecords = 0;
                    kvSets.clear();
                    HbaseUtils hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + Constants.KEY_HBASE_RECORDID_TBL_SUFFIX,
                            ConfigFactory.getHbaseConf());
                    HTable table = (HTable) hbaseUtils.getHtable();
                    new LoadIncrementalHFiles(ConfigFactory.getHbaseConf()).doBulkLoad(outPut, table);
                    writer = null;
                }
//                System.out.println(String.format("finished %.2f, %s", (finishedTasks.incrementAndGet() * 1.0) / totalPartitions, path));

            }
            if (writer != null) {
                writer.close();
                kvSets.clear();
                HbaseUtils hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + Constants.KEY_HBASE_RECORDID_TBL_SUFFIX,
                        ConfigFactory.getHbaseConf());
                HTable table = (HTable) hbaseUtils.getHtable();
                new LoadIncrementalHFiles(ConfigFactory.getHbaseConf()).doBulkLoad(outPut, table);
                hbaseUtils.close();
            }
        }
    }

    public void load(JavaSparkContext sparkContext) throws Exception {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", ConfigHolder.getConfig().getMetastoreUris());
        IMetaStoreClient iMetaStoreClient = new HiveMetaStoreClient(hiveConf);

        if (!prepare(iMetaStoreClient)) {
            return;
        }

        PartitionSpecProxy proxy = iMetaStoreClient.listPartitionSpecs(db, table, Integer.MAX_VALUE);
        Iterator<Partition> iterator = proxy.getPartitionIterator();

        FileSystem fs = FileSystem.get(new Configuration());
        Path tablePath = new Path(iMetaStoreClient.getTable(db, table).getSd().getLocation());
        //单位：B
        long spaceBytes = fs.getContentSummary(tablePath).getSpaceConsumed();
        final long mb = spaceBytes / 3 / 1024 / 1024;
        System.out.println(table + "storage size: " + mb + "mb");

        sparkConf.set("table_size", Long.toString(mb));

        List<String> paths = new ArrayList<>();
        while (iterator.hasNext()) {
            String path = iterator.next().getSd().getLocation();
            if (!fs.exists(new Path(path))) {
                continue;
            }
            totalPartitions++;
            paths.add(path);
        }

        if (paths.isEmpty()) {
            return;
        }

        System.out.println("table: " + table + ", total partitions: " + totalPartitions);
        sparkContext.parallelize(paths, cores).foreachPartition(new LoadFunction());


        iMetaStoreClient.close();
        System.out.println("finished loading table: " + table);
    }

    private boolean prepare(IMetaStoreClient iMetaStoreClient) throws Exception {
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

        //found primaryKey
        Properties properties = new Properties();
        properties.load(RecordIdLoaderTools.class.getResourceAsStream(Constants.MYSQL_DB_CONF_FILE));
        String mysqlUrl = properties.getProperty(db);
        String user = properties.getProperty(Constants.MYSQL_DB_USER);
        String password = properties.getProperty(Constants.MYSQL_DB_PASSWORD);

        this.primaryKeyIndex = getPrimaryKeyIndexs(mysqlUrl, user, password);
        this.updateTimeIndex = getUpdateTimeIndex();

        if (primaryKeyIndex.isEmpty()) {
            return false;
        }

        if (updateTimeIndex == null) {
            return false;
        }

        if (CollectionUtils.isEmpty(primaryKeyIndex)) {
            System.out.println("primary key not found for table: " + db + "." + table);
            return false;
        }

        return true;
    }

    private Integer getUpdateTimeIndex() {
        String updateTimeCol = RecordUtils.getCreateTimeCol(db + "." + table, columnList);
        if (StringUtils.isEmpty(updateTimeCol)) {
            System.out.println(String.format("update time not found for table: %s", table));
            return null;
        }
        System.out.println(String.format("update time for table: %s, is %s", table, updateTimeCol));
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
}