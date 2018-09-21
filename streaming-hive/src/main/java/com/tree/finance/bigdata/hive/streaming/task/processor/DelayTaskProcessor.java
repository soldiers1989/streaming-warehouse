package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
import com.tree.finance.bigdata.hive.streaming.task.listener.DbTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import io.prometheus.client.Summary;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/19 15:20
 */
public class DelayTaskProcessor {

    private AppConfig config = ConfigHolder.getConfig();

    private static Logger LOG = LoggerFactory.getLogger(DelayTaskProcessor.class);

    private ConnectionFactory factory = ConfigHolder.getDbFactory();

    private DbTaskStatusListener dbTaskStatusListener = new DbTaskStatusListener(factory);

    private LinkedBlockingQueue<MysqlTask> taskQueue = new LinkedBlockingQueue<>(10);

    private Thread thread;

    private volatile boolean stop = false;

    private int id;

    public DelayTaskProcessor(int id) {
        this.id = id;
        this.thread = new Thread(this::run, "DelayTaskProcessor-" + id);
    }

    public void init() {
        this.thread.start();
    }

    public void run() {
        while (!stop || !taskQueue.isEmpty()) {
            try {
                handleDelayTask(taskQueue.take());
            } catch (InterruptedException e) {
                // no opt
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    public void handleDelayTask(MysqlTask task) {

        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path path = new Path(task.getTaskInfo().getFilePath());
            if (!fileSystem.exists(path)) {
                LOG.info("path not exist: {}", path);
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
                return;
            }
        } catch (Exception e) {
            LOG.error("maybe hadoop file system unavailable, skip task", e);
        }

        //handle as insert, and last time handle as update
        if (task.getTaskInfo().getAttempt() == (config.getDelayTaskMaxRetries() - 1)) {
            handleAsInsertTask(task);
        } else {
            handleAsUpdateTask(task);
        }
    }

    private void handleAsInsertTask(MysqlTask task) {
        InsertMutation mutationUtils = new InsertMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("delay task start as insert : {}", task.getTaskInfo().getFilePath());
            try (AvroFileReader reader = new AvroFileReader(new Path(task.getTaskInfo().getFilePath()))) {
                Schema recordSchema = reader.getSchema();
                mutationUtils.beginFixTransaction(recordSchema, ConfigHolder.getHiveConf());
                while (reader.hasNext()) {
                    GenericData.Record record = reader.next();
                    mutationUtils.insert(record);
                }
            }
            mutationUtils.commitTransaction();
            //will only insert records that not exist, so should handle this update task again, for the rest exist update record
            dbTaskStatusListener.onTaskDelay(task.getTaskInfo());
            LOG.info("delay task as insert success : {}, cost: {}ms", task.getTaskInfo().getFilePath(),
                    System.currentTimeMillis() - start);
        } catch (TransactionException e) {
            try {
                if (e.getCause() instanceof LockException) {
                    LOG.warn("failed to lock for mq task, txnId: {}, file: {}", mutationUtils.getTransactionId(),
                            task.getTaskInfo().getFilePath());
                } else {
                    LOG.error("txn exception", e);
                }
                mutationUtils.abortTxn();
            } catch (Exception ex) {
                LOG.error("error handle transaction exception", ex);
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

    public void handleAsUpdateTask(MysqlTask task) {
        Summary.Timer timer = MetricReporter.startUpdate();
        long startTime = System.currentTimeMillis();
        LOG.info("delay task start: {}", task.getTaskInfo().getFilePath());
        UpdateMutation updateMutation = new UpdateMutation(task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(),
                task.getTaskInfo().getPartitionName(), task.getTaskInfo().getPartitions(),
                config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            Path path = new Path(task.getTaskInfo().getFilePath());
            try (AvroFileReader reader = new AvroFileReader(path);) {
                Schema recordSchema = reader.getSchema();
                updateMutation.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
                while (reader.hasNext()) {
                    GenericData.Record record = reader.next();
                    updateMutation.update(record, false);
                }
            }
            updateMutation.commitTransaction();
            dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            long endTime = System.currentTimeMillis();
            LOG.info("delay task success: {} cost: {}ms", task.getTaskInfo().getFilePath(), endTime - startTime);

        } catch (DataDelayedException e) {
            //no need to update task status to delay again
            LOG.info("task delay again: {}", task.getTaskInfo());
            dbTaskStatusListener.onTaskDelay(task.getTaskInfo());
        } catch (TransactionException e) {
            if (e.getCause() instanceof LockException) {
                try {
                    LOG.warn("failed to lock for delay mysql task, txnId: {}, file: {}", updateMutation.getTransactionId(), task.getTaskInfo().getFilePath());
                    // not update info in database
                } catch (Exception ex) {
                    LOG.error("failed to process lock exception", ex);
                }
            } else {
                LOG.error("caught txn exception", e);
                // not update info in database
            }
            updateMutation.abortTxn();
        } catch (Throwable t) {
            LOG.error("file task failed: " + task.getTaskInfo().getFilePath(), t);
            try {
                updateMutation.abortTxn();
                dbTaskStatusListener.onTaskError(task.getTaskInfo());
            } catch (Exception e) {
                LOG.error("error abort txn", e);
            }
        } finally {
            try {
                timer.observeDuration();
            } catch (Exception e) {
                LOG.error("error closing client", e);
            }
        }
    }

    public void process(MysqlTask consumedTask) {
        this.taskQueue.offer(consumedTask);
    }

    public void stop() {
        this.stop = true;
        this.thread.interrupt();
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping DelayTaskProcessor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped DelayProcessor-{}", id);
    }
}
