package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.constants.DynamicConfig;
import com.tree.finance.bigdata.hive.streaming.mutation.AvroMutationFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.HiveLockFailureListener;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import com.tree.finance.bigdata.utils.common.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_RECORDID_TBL_SUFFIX;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/27 14:24
 */
public abstract class Mutation {

    protected static final int BUCKET_ID = 0;
    protected static final byte[] columnFamily = Bytes.toBytes(ConfigFactory.getHbaseColumnFamily());
    protected static final byte[] recordIdColIdentifier = Bytes.toBytes(ConfigFactory.getHbaseRecordIdColumnIdentifier());
    protected static final byte[] updateTimeColIdentifier = Bytes.toBytes(ConfigFactory.getHbaseUpdateTimeColumnIdentifier());
    private static Logger LOG = LoggerFactory.getLogger(Mutation.class);

    protected MutatorClient mutatorClient;

    protected MutatorCoordinator mutateCoordinator;

    protected Transaction mutateTransaction;

    protected MutatorFactory factory;

    protected long rowId = 0;

    protected boolean txnOpen = false;

    protected String metastoreUris;

    protected long transactionId;

    protected String db;
    protected String table;
    protected String partition;
    protected List<String> partitions;

    protected Schema recordSchema;

    protected HbaseUtils hbaseUtils;

    protected Configuration hbaseConf;

    protected String updateCol;

    protected boolean checkExist;

    protected DynamicConfig dynamicConfig;

    protected Long latestUpdateTime;

    protected long mutateRecords;

    protected Object2ObjectMap<String, RecordIdentifier> bizToRecIdMap =  new Object2ObjectOpenHashMap<>();

    protected Mutation(String db, String table, String partition, List<String> partitions, String metastoreUris,
                       Configuration hbaseConf) {
        this.metastoreUris = metastoreUris;
        this.db = db;
        this.table = table;
        this.partition = partition;
        this.partitions = partitions;
        this.hbaseConf = hbaseConf;
    }

    public boolean txnOpen() {
        return txnOpen;
    }

    public long commitTransaction() throws Exception {
        if (!txnOpen()) {
            return 0;
        }
        closeHbaseUtil();
        closeMutator();
        if (null != mutateTransaction) {
            mutateTransaction.commit();
        }
        closeClientQueitely();

        if (null != latestUpdateTime && dynamicConfig != null) {
            try {
                dynamicConfig.refreshStreamTime(db, table, partition, Long.toString(latestUpdateTime));
            } catch (Throwable e) {
                LOG.warn("error update fix update_time table, may cause program efficiency problem when fixing repair.", e);
            }
        }
        closeDynamicConfigQuietely();

        return mutateRecords;

    }

    protected void closeMutator() throws IOException{
        if (null != mutateCoordinator) {
            mutateCoordinator.close();
        }
    }

    private void closeHbaseUtil() throws IOException {
        if (null != hbaseUtils) {
            hbaseUtils.close();
        }
    }

    private void closeMutatorQuietly() {
        try {
            if (null != this.mutateCoordinator) {
                this.mutateCoordinator.close();
            }
        } catch (Exception e) {
            //no opt
        }
    }

    public void abortTxn() {
        if (!txnOpen()) {
            return;
        }
        closeMutatorQuietly();
        try {
            if (mutateTransaction != null) {
                mutateTransaction.abort();
            }
        } catch (Exception e) {
            //no pot
        }
        closeHbaseUtilQuietly();
        closeClientQueitely();
        closeDynamicConfigQuietely();
    }

    protected void closeDynamicConfigQuietely() {
        try {
            if (null != dynamicConfig && null != dynamicConfig.getHbaseUtils()){
                dynamicConfig.getHbaseUtils().close();
            }
        } catch (Exception e) {
            //no opt
        }
    }

    private void closeHbaseUtilQuietly() {
        try {
            if (null != hbaseUtils) {
                hbaseUtils.close();
            }
        } catch (Exception e) {
            //no opts
        }
    }

    private void closeClientQueitely() {
        try {
            if (null != mutatorClient) {
                mutatorClient.close();
            }
        } catch (Exception e) {
            //no pots
        }
    }

    protected void beginTransaction(Schema schema, HiveConf conf) throws Exception {
        this.updateCol = RecordUtils.getUpdateCol(db + "." + table, schema);
        if (StringUtils.isEmpty(updateCol)) {
            LOG.error("update column not found for table: {}, schema: {}", db + "." + table, schema);
            throw new RuntimeException("update column not found");
        }

        if (null == this.hbaseUtils) {
            this.hbaseUtils = HbaseUtils.getTableInstance(db + "." + table + KEY_HBASE_RECORDID_TBL_SUFFIX, hbaseConf);
        }

        this.recordSchema = schema;
        this.factory = new AvroMutationFactory(new Configuration(), new AvroObjectInspector(db,
                table, schema, bizToRecIdMap));
        this.mutatorClient = new MutatorClientBuilder()
                .configuration(conf)
                .lockFailureListener(new HiveLockFailureListener())
                .addSinkTable(db, table, partition, true)
                .metaStoreUri(metastoreUris)
                .build();

        long start = System.currentTimeMillis();
        this.mutatorClient.connect();
        LOG.info("client connect cost: {}ms", System.currentTimeMillis() - start);

        long start2 = System.currentTimeMillis();
        this.mutateTransaction = mutatorClient.newTransaction();
        LOG.info("new transaction cost: {}ms", System.currentTimeMillis() - start2);

        long lockStart = System.currentTimeMillis();
        this.transactionId = mutateTransaction.getTransactionId();
        //once we got new transaction, set initialized even when this transaction have not begun
        this.txnOpen = true;
        this.mutateTransaction.begin();
        LOG.info("begin transaction: {}, cost: {}ms", transactionId, System.currentTimeMillis() - lockStart);

        List<AcidTable> destinations = mutatorClient.getTables();
        this.mutateCoordinator = new MutatorCoordinatorBuilder()
                .metaStoreUri(metastoreUris)
                .table(destinations.get(0))
                .mutatorFactory(this.factory)
                .build();
    }

    public void beginStreamTransaction(Schema schema, HiveConf conf) throws Exception {
        beginTransaction(schema, conf);
        this.dynamicConfig = new DynamicConfig();

        Long[] streamAndFixParTime = dynamicConfig.getPartitionUpdateTimes(db, table, partition);
        Long[] streamAndFixTblTime = dynamicConfig.getTableUpdateTimes(db, table);
        latestUpdateTime = streamAndFixParTime[0];

        //stream table global update time not set, means first insert, should check
        if (null == streamAndFixTblTime[0]) {
            LOG.info("table update time is null, may be first op, should check update time when insert");
            this.checkExist = true;
            return;
        }
        //streaming program's global update time, earlier than fix global update time, should check
        else if (null != streamAndFixTblTime[1] && streamAndFixTblTime[1] > streamAndFixTblTime[0]) {
            LOG.info("check update time when insert,  global stream_update_time: {}, global check_update_time: {}",
                    streamAndFixTblTime[0], streamAndFixTblTime[1]);
            this.checkExist = true;
            return;
        }
        //if fix program fix at partition level
        else if (streamAndFixParTime[1] != null) {
            if (null == streamAndFixParTime[0] || streamAndFixParTime[0]
                    < streamAndFixParTime[1]) {
                this.checkExist = true;
                return;
            }
        }
        this.checkExist = false;
        return;
    }

    public void beginFixTransaction(Schema schema, HiveConf conf) throws Exception {
        beginTransaction(schema, conf);
        this.checkExist = true;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
