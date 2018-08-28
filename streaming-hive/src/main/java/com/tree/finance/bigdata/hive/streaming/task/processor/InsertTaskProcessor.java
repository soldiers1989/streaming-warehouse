package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.Constants;
import com.tree.finance.bigdata.hive.streaming.mutation.AvroMutationFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.listener.HiveLockFailureListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.TaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.utils.common.StringUtils;
import com.tree.finance.bigdata.hive.streaming.utils.hbase.HbaseUtils;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.hive.streaming.utils.record.RecordUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.AVRO_KEY_RECORD_ID;
import static com.tree.finance.bigdata.hive.streaming.config.Constants.FILE_PROCESSED_SUFIX;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/9 20:05
 */
public class InsertTaskProcessor implements Runnable {

    private AppConfig config;

    private static TaskStatusListener taskStatusListener;

    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private static final String BUCKET_ID = "0";

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

    private byte[] columnFamily;
    private byte[] recordIdColIdentifier;
    private byte[] updateTimeColIdentifier;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    private final int BATCH_TRIGGER_QUEUE_SIZE;

    InsertTaskProcessor(AppConfig config, int id) {
        this.config = config;
        BATCH_TRIGGER_QUEUE_SIZE = config.getBatchTriggerQueueSize();
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.taskStatusListener = new MqTaskStatusListener(config);

        this.columnFamily = Bytes.toBytes(config.getDefaultColFamily());
        this.recordIdColIdentifier = Bytes.toBytes(config.getRecordIdQualifier());
        this.updateTimeColIdentifier = Bytes.toBytes(config.getUpdateTimeQualifier());

        this.thread = new Thread(this, "Insert-Processor-" + id);
    }

    public void init() {
        thread.start();
    }

    @Override


    public void run() {

        while (!stop || !taskQueue.isEmpty()) {
            try {
                if (taskQueue.size() >= BATCH_TRIGGER_QUEUE_SIZE) { // too much task, group first
                    LOG.info("queue size [{}] exceed [{}] will process in batch", taskQueue.size(), BATCH_TRIGGER_QUEUE_SIZE);
                    Map<LockComponent, List<ConsumedTask>> map = new HashMap<>();

                    for (int i = 0; i < BATCH_TRIGGER_QUEUE_SIZE; i++) {
                        ConsumedTask task = taskQueue.take();
                        LockComponent component = new LockComponent(task.getTaskInfo().getDb(),
                                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName());
                        if (!map.containsKey(component)) {
                            map.put(component, new ArrayList<>());
                        }
                        List<ConsumedTask> taskInfos = map.get(component);
                        taskInfos.add(task);
                    }
                    for (List<ConsumedTask> value : map.values()) {
                        handleByBatch(value, 2);
                    }
                } else {
                    ConsumedTask task = taskQueue.take();
                    handleByEach(task, 2);
                }
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    private void handleByBatch(List<ConsumedTask> tasks, int remainingRetries) {

        long start = System.currentTimeMillis();

        LOG.info("start batch tasks: [{}.{}.{}], task size: {}", tasks.get(0).getTaskInfo().getDb(),
                tasks.get(0).getTaskInfo().getTbl(), tasks.get(0).getTaskInfo().getPartitionName(), tasks.size());

        HbaseUtils hbaseUtil = null;
        MutationUtils mutationUtils = new MutationUtils();

        try {
            hbaseUtil = HbaseUtils.getTableInstance(config.getRowIdToRecIdHbaseTbl());
            int i = 0;

            for (ConsumedTask task : tasks) {
                i++;
                long startTime = System.currentTimeMillis();
                LOG.info("batch file task {} start: {}", i, task.getTaskInfo().getFilePath());
                handleTask(hbaseUtil, mutationUtils, task);
                LOG.info("batch file task {} success: {} cost: {}ms", i, task.getTaskInfo().getFilePath()
                        , System.currentTimeMillis() - startTime);
            }

            //should not ignore HBase client closing error, to prevent from rowKey not write properly
            hbaseUtil.close();
            mutationUtils.closeMutator();
            mutationUtils.getMutateTransaction().commit();
            mutationUtils.closeClientQueitely();

            try {
                taskStatusListener.onTaskSuccess(tasks);
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.error("ack success failed, will not affect data accuracy", e);
            }

            LOG.info("end batch tasks: {}.{}.{}, task size: {}, cost: {}ms", tasks.get(0).getTaskInfo().getDb(),
                    tasks.get(0).getTaskInfo().getTbl(), tasks.get(0).getTaskInfo().getPartitionName(), tasks.size(),
                    System.currentTimeMillis() - start);

        } catch (TransactionException e) { // if get transaction exception, retry in batch, or send to retry queue when exceed max retries
            try {
                try {
                    mutationUtils.closeClientQueitely();
                    mutationUtils.abortTxnQuietly();
                    closeQuietly(hbaseUtil);
                    mutationUtils.closeClientQueitely();
                } catch (Exception ex) {
                    LOG.warn("error closing", e);
                }

                if (--remainingRetries > 0) {
                    LOG.warn("get transaction error in batch, remaining retries: {}", remainingRetries, e);
                    handleByBatch(tasks, remainingRetries);
                } else {
                    LOG.warn("get transaction error in batch, will not retry, resent to retry queue, task sample: {}", tasks.get(0), e);
                    taskStatusListener.onTaskRetry(tasks);
                }

            } catch (Exception ex) {
                LOG.error("error", ex);
            }
        } catch (Throwable t) { //if other error try batch by single
            LOG.error("batch file task failed, will try by each, task sample: {}" + tasks.get(0), t);
            mutationUtils.closeMutatorQuietly();
            mutationUtils.abortTxnQuietly();
            closeQuietly(hbaseUtil);
            mutationUtils.closeClientQueitely();
            for (ConsumedTask task : tasks) {
                handleByEach(task, 2);
            }
        }
    }

    private void closeQuietly(HbaseUtils hbaseUtil) {
        try {
            if (null != hbaseUtil) {
                hbaseUtil.close();
            }
        } catch (Exception e) {
            //no opts
        }
    }

    private void handleByEach(ConsumedTask task, int remainningRetry) {

        MutationUtils mutationUtils = new MutationUtils();
        HbaseUtils hbaseUtil = null;
        try {
            hbaseUtil = HbaseUtils.getTableInstance(config.getRowIdToRecIdHbaseTbl());

            long start = System.currentTimeMillis();
            LOG.info("single task start: {}", task.getTaskInfo().getFilePath());
            handleTask(hbaseUtil, mutationUtils, task);
            LOG.info("single task success: {}, cost: {}ms", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);

            hbaseUtil.close();
            mutationUtils.closeMutator();
            mutationUtils.commitTransaction();
            try {
                taskStatusListener.onTaskSuccess(task);
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.error("ack success failed, will not affect data accuracy", e);
            }

        } catch (TransactionException e) { // if get transaction exception retry batch, or send to retry queue

            mutationUtils.closeMutatorQuietly();
            mutationUtils.abortTxnQuietly();
            closeQuietly(hbaseUtil);
            mutationUtils.closeClientQueitely();

            if (--remainningRetry > 0) {
                long start = System.currentTimeMillis();
                LOG.info("single task retry start: {}, remaining retries: {}", task.getTaskInfo().getFilePath(), remainningRetry);
                handleByEach(task, remainningRetry);
                LOG.info("single task retry end: {}, cost: {}", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);
            } else {
                LOG.warn("retry exceed limit resent to retry queue: {}", task, e);
                taskStatusListener.onTaskRetry(task);
            }

        } catch (Throwable t) { //if other error try batch by single
            LOG.error("file task failed, will send to error queue: {}", task.getTaskInfo(), t);
            try {
                mutationUtils.closeMutatorQuietly();
                mutationUtils.abortTxnQuietly();
                closeQuietly(hbaseUtil);
                mutationUtils.closeClientQueitely();
                taskStatusListener.onTaskError(task);
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }


    private void handleTask(HbaseUtils hbaseUtil, MutationUtils mutationUtils, ConsumedTask task) throws Exception {

        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(task.getTaskInfo().getFilePath());
        String dbTable = task.getTaskInfo().getDb() + "." + task.getTaskInfo().getTbl();

        if (!fileSystem.exists(path)) {
            if (fileSystem.exists(new Path(task.getTaskInfo().getFilePath() + FILE_PROCESSED_SUFIX))) {
                LOG.warn("duplicate task: {}", task);
            } else {
                LOG.warn("path not exist: {}", task.getTaskInfo().getFilePath());
            }
            return;
        }

        try (AvroFileReader reader = new AvroFileReader(new Path(task.getTaskInfo().getFilePath()))) {
            Schema recordSchema = reader.getSchema();
            if (!mutationUtils.initialized()) {

                mutationUtils.setFactory(new AvroMutationFactory(new Configuration(), new AvroObjectInspector(task.getTaskInfo().getDb(),
                        task.getTaskInfo().getTbl(), recordSchema)));

                mutationUtils.setMutatorClient(new MutatorClientBuilder()
                        .lockFailureListener(new HiveLockFailureListener())
                        .addSinkTable(task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(), true)
                        .metaStoreUri(config.getMetastoreUris())
                        .build());
                mutationUtils.getMutatorClient().connect();

                mutationUtils.setMutateTransaction(mutationUtils.getMutatorClient().newTransaction());
                mutationUtils.getMutateTransaction().begin();

                List<AcidTable> destinations = mutationUtils.getMutatorClient().getTables();
                mutationUtils.setMutateCoordinator(new MutatorCoordinatorBuilder()
                        .metaStoreUri(config.getMetastoreUris())
                        .table(destinations.get(0))
                        .mutatorFactory(mutationUtils.getFactory())
                        .build());

                mutationUtils.setInitialized();
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();
            long transactionId = mutationUtils.getMutateTransaction().getTransactionId();
            List<String> partitions = task.getTaskInfo().getPartitions();
            String dbTblPrefix = dbTable + '_';
            MutatorCoordinator coordinator = mutationUtils.getMutateCoordinator();
            String updateColumn = RecordUtils.getUpdateCol(dbTable, recordSchema);

            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                GenericData.Record keyRecord = (GenericData.Record) record.get(Constants.AVRO_KEY_RECORD_ID);
                String recordId = transactionId + "_" + BUCKET_ID + "_" + (mutationUtils.incAndReturnRowId());
                Put put = new Put(Bytes.toBytes(dbTblPrefix +
                        GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(AVRO_KEY_RECORD_ID).schema()))
                );
                put.addColumn(columnFamily, recordIdColIdentifier, Bytes.toBytes(recordId));
                if (!StringUtils.isEmpty(updateColumn)) {
                    Long updateTime = RecordUtils.getFieldAsTimeMillis(updateColumn, record);
                    if (null != updateColumn) {
                        put.addColumn(columnFamily, updateTimeColIdentifier, Bytes.toBytes(updateTime));
                    }
                }
                hbaseUtil.insertAsync(put);
                coordinator.insert(partitions, record);
            }
            MetricReporter.insertedBytes(bytes);
        }

    }


    public void process(ConsumedTask consumedTask) {
        this.taskQueue.offer(consumedTask);
    }

    public void stop() {
        this.stop = true;
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping Insert-Processor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped Insert-Processor-{}", id);
    }
}
