package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.task.TaskInfo;
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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/17 10:28
 */
public class UpdateTaskProcessor extends TaskProcessor implements Runnable {

    private AppConfig config;

    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    public UpdateTaskProcessor(AppConfig config, ConnectionFactory factory, int id) {
        super(config, factory);
        this.config = config;
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.thread = new Thread(this, "Update-Processor-" + id);
    }

    public void init() {
        this.thread.start();
    }

    @Override
    public void run() {
        while (!stop || !taskQueue.isEmpty()) {
            try {
                ConsumedTask task = null;
                try {
                    task = taskQueue.take();
                } catch (InterruptedException e) {
                    //no opt
                }
                if (task == null) {
                    continue;
                }
                if (handle(task)) {
                    handleMoreTask(task.getTaskInfo());
                }

            } catch (Throwable t) {
                LOG.error("unexpected error", t);
            }
        }
        LOG.info("Update-Processor-{} stopped", id);
    }

    private boolean handle(ConsumedTask task) {
        Summary.Timer timer = MetricReporter.startUpdate();
        long startTime = System.currentTimeMillis();
        LOG.info("file task start: {}", task.getTaskInfo().getFilePath());
        UpdateMutation updateMutation = new UpdateMutation(task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(),
                task.getTaskInfo().getPartitionName(), task.getTaskInfo().getPartitions(),
                config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            Path path = new Path(task.getTaskInfo().getFilePath());
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if (!fileSystem.exists(path)) {
                LOG.warn("path not exist: {}", task.getTaskInfo().getFilePath());
                mqTaskStatusListener.onTaskSuccess((RabbitMqTask) task);
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
                return false;
            }

            long records = 0l;
            try (AvroFileReader reader = new AvroFileReader(path)) {
                Schema recordSchema = reader.getSchema();
                updateMutation.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
                while (reader.hasNext()) {
                    GenericData.Record record = reader.next();
                    updateMutation.update(record);
                    records++;
                }
            }
            updateMutation.commitTransaction();
            mqTaskStatusListener.onTaskSuccess((RabbitMqTask) task);
            dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            MetricReporter.updateFiles(1);
            long endTime = System.currentTimeMillis();
            LOG.info("file task success: {}, records: {}, cost: {}ms", task.getTaskInfo().getFilePath(), records, endTime - startTime);
            return true;

        } catch (DataDelayedException e) {
            LOG.info("task delay: {}", task.getTaskInfo().getFilePath());
            mqTaskStatusListener.onTaskDelay((RabbitMqTask) task);
            dbTaskStatusListener.onTaskDelay(task.getTaskInfo());
            updateMutation.abortTxn();
        } catch (TransactionException e) {
            if (e.getCause() instanceof LockException) {
                try {
                    LOG.warn("failed to lock for mq task, txnId: {}, file: {}", updateMutation.getTransactionId(), task.getTaskInfo().getFilePath());
                    // not update info in database, but ack mq message
                } catch (Exception ex) {
                    LOG.error("failed to process lock exception", ex);
                } finally {
                    updateMutation.abortTxn();
                    mqTaskStatusListener.onTaskError((RabbitMqTask) task);
                }
            } else {
                LOG.warn("caught txn exception, let others process this task", e);
                updateMutation.abortTxn();
                mqTaskStatusListener.onTaskError((RabbitMqTask) task);
                // not update info in database, but ack mq message
            }
        } catch (Throwable t) {
            LOG.error("file task failed: " + task.getTaskInfo().getFilePath(), t);
            try {
                updateMutation.abortTxn();
                mqTaskStatusListener.onTaskError((RabbitMqTask) task);
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
        return false;
    }

    public void process(ConsumedTask consumedTask) {
        try {
            while (!this.taskQueue.offer(consumedTask, 10, TimeUnit.SECONDS)) {
                LOG.warn("task queue full");
            }
        } catch (InterruptedException e) {
            //ignore
        }
    }

    @Override
    protected void handleMoreTask(TaskInfo previousTask) {
        List<TaskInfo> moreTasks = getSameTask(previousTask);
        UpdateMutation updateMutation = null;
        LOG.info("going to handle {} more tasks", moreTasks.size());
        for (TaskInfo task : moreTasks) {
            Summary.Timer timer = MetricReporter.startUpdate();
            long startTime = System.currentTimeMillis();
            LOG.info("additional file task start: {}", task.getFilePath());
            updateMutation = new UpdateMutation(task.getDb(), task.getTbl(),
                    task.getPartitionName(), task.getPartitions(),
                    config.getMetastoreUris(), ConfigFactory.getHbaseConf());
            try {
                Path path = new Path(task.getFilePath());
                FileSystem fileSystem = FileSystem.get(new Configuration());
                if (!fileSystem.exists(path)) {
                    LOG.warn("additional path not exist: {}", task.getFilePath());
                    dbTaskStatusListener.onTaskSuccess(task);
                    return;
                }
                long records = 0;
                try (AvroFileReader reader = new AvroFileReader(path);) {
                    Schema recordSchema = reader.getSchema();
                    updateMutation.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
                    while (reader.hasNext()) {
                        GenericData.Record record = reader.next();
                        updateMutation.update(record);
                        records++;
                    }
                }
                updateMutation.commitTransaction();
                MetricReporter.updateFiles(1);
                dbTaskStatusListener.onTaskSuccess(task);
                long endTime = System.currentTimeMillis();
                LOG.info("additional file task success: {} records: {}, cost: {}ms", task.getFilePath(), records, endTime - startTime);

            } catch (TransactionException e) {
                if (e.getCause() instanceof LockException) {
                    try {
                        LOG.warn("failed to lock for more task, txnId: {}, file: {}", updateMutation.getTransactionId(), task.getFilePath());
                        // not update info in database, but ack mq message
                    } catch (Exception ex) {
                        LOG.error("failed to process lock exception", ex);
                    }
                } else {
                    LOG.error("caught txn exception", e);
                    // not update info in database, but ack mq message
                }
                updateMutation.abortTxn();
                return;
            } catch (DataDelayedException e) {
                LOG.info("additional task delayed: {}", task);
                dbTaskStatusListener.onTaskDelay(task);
                updateMutation.abortTxn();
            } catch (Throwable t) {
                LOG.error("additional file task failed: " + task.getFilePath(), t);
                try {
                    dbTaskStatusListener.onTaskError(task);
                    updateMutation.abortTxn();
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
    }

    public void stop() {
        this.stop = true;
        this.thread.interrupt();
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping Update-Processor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped Update-Processor-{}", id);
    }

}
