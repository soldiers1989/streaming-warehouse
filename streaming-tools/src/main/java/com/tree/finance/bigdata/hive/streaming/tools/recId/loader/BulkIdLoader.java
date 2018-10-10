package com.tree.finance.bigdata.hive.streaming.tools.recId.loader;

import com.google.common.collect.Lists;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.tools.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.tools.config.Constants;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import com.tree.finance.bigdata.hive.streaming.utils.RecordUtils;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/21 15:02
 */
public class BulkIdLoader {

    static final String SPACE_REPLACEMENT = "\\|";

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

    private final long cacheRecords = 10000000;

    private AtomicInteger finishedTasks = new AtomicInteger(0);

    private String parFilter;

    public BulkIdLoader(String db, String table, String parFilter, int cores) {
        this.cores = cores;
        this.db = db;
        this.table = table;
        this.parFilter = parFilter.replaceAll(SPACE_REPLACEMENT, " ");
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
        PartitionSpecProxy proxy;
        if (StringUtils.isEmpty(parFilter)) {
            proxy = iMetaStoreClient.listPartitionSpecs(db, table, Integer.MAX_VALUE);
        } else {
            proxy = iMetaStoreClient.listPartitionSpecsByFilter(db, table, parFilter, Integer.MAX_VALUE);
        }

        Iterator<Partition> iterator = proxy.getPartitionIterator();

        FileSystem fs = FileSystem.get(new Configuration());
        Path tablePath = new Path(iMetaStoreClient.getTable(db, table).getSd().getLocation());
        //单位：B
        long spaceBytes = fs.getContentSummary(tablePath).getSpaceConsumed();
        long spaceMB = spaceBytes / 3 / 1024 / 1024;
        System.out.println(table + "storage size: " + spaceMB + "mb");

        List<Path> paths = new ArrayList<>();
        while (iterator.hasNext()) {
            String path = iterator.next().getSd().getLocation();
            if (!fs.exists(new Path(path))) {
                continue;
            }
            totalPartitions++;
            paths.add(new Path(path));
        }

        if (paths.size() == 0) {
            return;
        }

        int splitSize = Double.valueOf(Math.ceil(paths.size() * 1.0 / cores)).intValue();
        System.out.println("paths: " + paths.size() + " ceil: " + splitSize);

        List<List<Path>> splits = Lists.partition(paths, splitSize);

        for (List<Path> split : splits) {
            executor.submit(new BulkIdLoader.LoadTask(split, spaceMB));
        }

        System.out.println("table: " + table + ", total partitions: " + totalPartitions);
        iMetaStoreClient.close();
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
        properties.load(RecordIdLoaderTools.class.getResourceAsStream(Constants.MYSQL_DB_CONF_FILE));
        String mysqlUrl = properties.getProperty(db);
        String user = properties.getProperty(Constants.MYSQL_DB_USER);
        String password = properties.getProperty(Constants.MYSQL_DB_PASSWORD);

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
        if (keyIndexes.isEmpty()) {
            throw new RuntimeException("not primary key found");
        }
        return keyIndexes;
    }

    private class LoadTask implements Runnable {

        private List<Path> paths;
        private long mb;
        private long wroteRecords = 0;

        public LoadTask(List<Path> paths, long mb) {
            this.paths = paths;
            this.mb = mb;
        }

        @Override
        public void run() {

            TreeSet<KeyValue> kvSets = new TreeSet<>(new KeyValue.KVComparator());
            StoreFile.Writer writer = null;
            Path outPut = null;

            for (Path path : paths) {
                try {
                    Configuration conf = new Configuration();
                    conf.set("mapred.input.dir", path.toString());
                    conf.set("schema.evolution.columns", columns);
                    conf.set("schema.evolution.columns.types", types);
                    conf.setInt(hive_metastoreConstants.BUCKET_COUNT, 1);

                    if (writer == null) {
                        FileSystem fs = FileSystem.get(new Configuration());
                        outPut = new Path("/tmp/streaming-recId/" + db + "/" + table + "/" + UUID.randomUUID().getLeastSignificantBits());

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

                            if (busiIdSb.length() < 1) {
                                LOG.warn("ignore no empty pk value");
                                continue;
                            }

                            String rowKey = GenericRowIdUtils.addIdWithHash(busiIdSb.deleteCharAt(busiIdSb.length() - 1).toString());
                            Long updateTime = RecordUtils.getFieldAsTimeMillis(value.getFieldValue(updateTimeIndex));
                            if (null == updateTime) {
                                updateTime = 0l;
                            }
                            long current = System.currentTimeMillis();
                            kvSets.add(new KeyValue(Bytes.toBytes(rowKey), columnFamily, recordIdentifier, current,
                                    KeyValue.Type.Put, Bytes.toBytes(idSb.toString())));
                            kvSets.add(new KeyValue(Bytes.toBytes(rowKey), columnFamily, updateTimeIdentifier, current,
                                    KeyValue.Type.Put, Bytes.toBytes(updateTime)));
                            wroteRecords++;
                        }

//                        inner.close();
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
                    System.out.println(String.format("finished %.2f, %s !!!", (finishedTasks.incrementAndGet() * 1.0) / totalPartitions, path
                    ));
                } catch (Exception e) {
                    LOG.error("failed to load: {}", path, e);
                    System.out.println("ERROR: failed to load: " + path);
                }
            }

            System.out.println("to last phase");
            try {
                if (writer != null && !kvSets.isEmpty()) {
                    System.out.println("final flush");
                    for (KeyValue kv : kvSets) {
                        writer.append(kv);
                    }
                    writer.close();
                    kvSets.clear();
                    wroteRecords = 0;
                    HbaseUtils hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + Constants.KEY_HBASE_RECORDID_TBL_SUFFIX,
                            ConfigFactory.getHbaseConf());
                    HTable table = (HTable) hbaseUtils.getHtable();
                    new LoadIncrementalHFiles(ConfigFactory.getHbaseConf()).doBulkLoad(outPut, table);
                    hbaseUtils.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            }


        }
    }

}
