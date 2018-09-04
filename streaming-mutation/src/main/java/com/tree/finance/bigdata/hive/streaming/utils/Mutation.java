package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.mutation.AvroMutationFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.mutation.HiveLockFailureListener;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import com.tree.finance.bigdata.schema.SchemaConstants;
import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/27 14:24
 */
public abstract class Mutation {

    protected static final String BUCKET_ID = "0";
    protected static final byte[] columnFamily = Bytes.toBytes("f");
    protected static final byte[] recordIdColIdentifier = Bytes.toBytes("recordId");
    protected static final byte[] updateTimeColIdentifier = Bytes.toBytes("update_time");

    protected MutatorClient mutatorClient;

    protected MutatorCoordinator mutateCoordinator;

    protected Transaction mutateTransaction;

    protected MutatorFactory factory;

    protected long rowId = 0;

    protected boolean initialized = false;

    protected String metastoreUris;

    protected Long transactionId;

    protected String db;
    protected String table;
    protected String partition;
    protected List<String> partitions;

    protected Schema recordSchema;

    protected String dbTblPrefix;

    protected HbaseUtils hbaseUtils;

    protected Configuration hbaseConf;

    protected String updateCol;

    protected Mutation(String db, String table, String partition, List<String> partitions, String metastoreUris,
                       Configuration hbaseConf) {
        this.metastoreUris = metastoreUris;
        this.db = db;
        this.table = table;
        this.partition = partition;
        this.partitions = partitions;
        this.dbTblPrefix = db + "." + table + "_";
        this.hbaseConf = hbaseConf;
    }

    protected void setHbaseUtils(HbaseUtils hbaseUtils) {
        this.hbaseUtils = hbaseUtils;
    }

    public boolean initialized() {
        return initialized;
    }

    public void commitTransaction() throws Exception {
        //should not ignore HBase client closing error, to prevent from rowKey not write properly
        hbaseUtils.close();
        mutateCoordinator.close();
        mutateTransaction.commit();
        closeClientQueitely();
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

    public void beginTransaction(Schema schema) throws Exception {
        this.updateCol = RecordUtils.getUpdateCol(db + "." + table, schema);
        this.recordSchema = schema;
        this.factory = new AvroMutationFactory(new Configuration(), new AvroObjectInspector(db,
                table, schema, hbaseUtils));
        this.mutatorClient = new MutatorClientBuilder()
                .lockFailureListener(new HiveLockFailureListener())
                .addSinkTable(db, table, partition, true)
                .metaStoreUri(metastoreUris)
                .build();
        this.mutatorClient.connect();
        this.mutateTransaction = mutatorClient.newTransaction();
        this.mutateTransaction.begin();
        List<AcidTable> destinations = mutatorClient.getTables();
        this.mutateCoordinator = new MutatorCoordinatorBuilder()
                .metaStoreUri(metastoreUris)
                .table(destinations.get(0))
                .mutatorFactory(this.factory)
                .build();
        this.transactionId = mutateTransaction.getTransactionId();
        if (null != this.hbaseUtils) {
            this.hbaseUtils = HbaseUtils.getTableInstance(hbaseConf);
        }
        this.initialized = true;
    }

}
