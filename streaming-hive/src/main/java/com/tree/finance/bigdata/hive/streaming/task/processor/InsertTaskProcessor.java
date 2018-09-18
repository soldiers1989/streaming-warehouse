package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/9 20:05
 */
public class InsertTaskProcessor extends TaskProcessor implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private LinkedBlockingQueue<RabbitMqTask> taskQueue;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    public InsertTaskProcessor(AppConfig config, ConnectionFactory factory, int id) {
        super(config, factory);
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.thread = new Thread(this, "Insert-Processor-" + id);
    }

    public void init() {
        thread.start();
    }

    @Override


    public void run() {

        while (!stop || !taskQueue.isEmpty()) {
            try {
                RabbitMqTask task = taskQueue.take();
                handleMqTask(task);
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    private void handleMqTask(RabbitMqTask task) {
        InsertMutation mutationUtils = new InsertMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("single task start: {}", task.getTaskInfo().getFilePath());
            handleTask(mutationUtils, task.getTaskInfo());
            LOG.info("single task success: {}, cost: {}ms", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);
            mutationUtils.commitTransaction();
            try {
                mqTaskStatusListener.onTaskSuccess((RabbitMqTask) task);
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.warn("ack success failed, will not affect data accuracy", e);
            }
            handleMoreTask(task.getTaskInfo());
        } catch (TransactionException e) {
            if (e.getCause() instanceof LockException) {
                try {
                    LOG.warn("failed to lock for mq task, txnId: {}, file: {}", mutationUtils.getTransactionId(),
                            task.getTaskInfo().getFilePath());
                    mutationUtils.commitTransaction();
                    // not update info in database, but ack mq message
                } catch (Exception ex) {
                    mutationUtils.abortTxn();
                    LOG.error("failed to process lock exception", ex);
                }finally {
                    mqTaskStatusListener.onTaskError(task);
                }
            } else {
                LOG.error("caught txn exception", e);
                mutationUtils.abortTxn();
                // not update info in database, but ack mq message
                mqTaskStatusListener.onTaskError(task);
            }

        } catch (Throwable t) {
            LOG.error("file task failed: {}", task.getTaskInfo(), t);
            try {
                mutationUtils.abortTxn();
                mqTaskStatusListener.onTaskError(task);
                dbTaskStatusListener.onTaskError(task.getTaskInfo());
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }

    public void handleMysqlTask(MysqlTask task) {
        InsertMutation mutationUtils = new InsertMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("single task start: {}", task.getTaskInfo().getFilePath());
            handleTask(mutationUtils, task.getTaskInfo());
            LOG.info("single task success: {}, cost: {}ms", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);
            mutationUtils.commitTransaction();
            try {
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.warn("ack success failed, will not affect data accuracy", e);
            }
            handleMoreTask(task.getTaskInfo());
        } catch (TransactionException e) {
            if (e.getCause() instanceof LockException) {
                try {
                    LOG.warn("failed to lock for mysql task, txnId: {}, file {}",
                            mutationUtils.getTransactionId(), task.getTaskInfo().getFilePath());
                    mutationUtils.commitTransaction();
                    // not update info in database, but ack mq message
                } catch (Exception ex) {
                    mutationUtils.abortTxn();
                    LOG.error("failed to process lock exception", ex);
                }
            } else {
                LOG.error("caught txn exception", e);
                mutationUtils.abortTxn();
                // not update info in database, but ack mq message
            }
        } catch (Throwable t) {
            LOG.error("file task failed: {}", task.getTaskInfo(), t);
            try {
                mutationUtils.abortTxn();
                dbTaskStatusListener.onTaskError(task.getTaskInfo());
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }

    protected void handleMoreTask(TaskInfo task) {
        List<TaskInfo> moreTasks;
        int greedyCount = config.getGreedyProcessBatchLimit();
        TaskInfo processing = null;
        while (!(moreTasks = getSameTask(task)).isEmpty()) {
            InsertMutation mutationUtils = new InsertMutation(task.getDb(),
                    task.getTbl(), task.getPartitionName(),
                    task.getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
            try {
                LOG.info("going to process {} more tasks", moreTasks.size());
                List<TaskInfo> processed = new ArrayList<>();

                for (TaskInfo sameTask : moreTasks) {
                    processing = sameTask;
                    if (processed.size() >= greedyCount) {
                        mutationUtils.commitTransaction();
                        LOG.info("processed {} additional tasks, committed as a batch", processed.size());
                        dbTaskStatusListener.onTaskSuccess(processed);
                        mutationUtils = new InsertMutation(task.getDb(),
                                task.getTbl(), task.getPartitionName(),
                                task.getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
                        processed.clear();
                    }
                    handleTask(mutationUtils, sameTask);
                    LOG.info("additional task success in batch: {}", sameTask.getFilePath());
                    processed.add(sameTask);
                }
                if (!processed.isEmpty()) {
                    mutationUtils.commitTransaction();
                    LOG.info("processed {} tasks, committed as a batch", processed.size());
                    dbTaskStatusListener.onTaskSuccess(processed);
                }
                LOG.info("finished processing {} more tasks", moreTasks.size());
            } catch (TransactionException e) {
                if (e.getCause() instanceof LockException) {
                    try {
                        LOG.warn("failed to lock for more task, txnId: {}, file: {}", mutationUtils.getTransactionId(), task.getFilePath());
                        mutationUtils.commitTransaction();
                        // not update info in database, but ack mq message
                    } catch (Exception ex) {
                        mutationUtils.abortTxn();
                        LOG.error("failed to process lock exception", ex);
                    }
                } else {
                    LOG.error("caught txn exception", e);
                    mutationUtils.abortTxn();
                    // not update info in database, but ack mq message
                }
            } catch (Throwable t) { //if other error try batch by single
                LOG.error("file task failed, {}", processing);
                mutationUtils.abortTxn();
                dbTaskStatusListener.onTaskError(processing);
            }
        }
    }


    private void handleTask(InsertMutation mutationUtils, TaskInfo task) throws Exception {

        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(task.getFilePath());

        if (!fileSystem.exists(path)) {
            LOG.info("path not exist: {}", task.getFilePath());
            return;
        }

        try (AvroFileReader reader = new AvroFileReader(new Path(task.getFilePath()))) {
            Schema recordSchema = reader.getSchema();
            if (!mutationUtils.txnStarted()) {
                mutationUtils.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                mutationUtils.insert(record);
            }
            MetricReporter.insertedBytes(bytes);
        }

    }


    public void process(RabbitMqTask consumedTask) {
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
