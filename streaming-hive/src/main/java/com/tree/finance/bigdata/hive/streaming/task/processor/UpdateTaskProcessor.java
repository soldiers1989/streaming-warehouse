package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
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
                    Thread.currentThread().interrupt();
                }
                if (task == null) {
                    continue;
                }
                handle(task);
                handleMoreTask(task.getTaskInfo());
            } catch (Throwable t) {
                LOG.error("unexpected error", t);
            }
        }
        LOG.info("Update-Processor-{} stopped", id);
    }

    private void handle(ConsumedTask task) {
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
                return;
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();

            try (AvroFileReader reader = new AvroFileReader(path)) {
                Schema recordSchema = reader.getSchema();
                updateMutation.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
                while (reader.hasNext()) {
                    GenericData.Record record = reader.next();
                    updateMutation.update(record, false);
                }
            }

            updateMutation.commitTransaction();
            mqTaskStatusListener.onTaskSuccess((RabbitMqTask) task);
            dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            MetricReporter.updatedBytes(bytes);
            long endTime = System.currentTimeMillis();
            LOG.info("file task success: {} cost: {}ms", task.getTaskInfo().getFilePath(), endTime - startTime);

        } catch (DataDelayedException e) {
            LOG.info("task delay: {}", task.getTaskInfo());
            mqTaskStatusListener.onTaskDelay((RabbitMqTask) task);
            dbTaskStatusListener.onTaskDelay(task.getTaskInfo());
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
    }

    public void handleDelayTask(MysqlTask task) {
        Summary.Timer timer = MetricReporter.startUpdate();
        long startTime = System.currentTimeMillis();
        LOG.info("mysql file task start: {}", task.getTaskInfo().getFilePath());
        UpdateMutation updateMutation = new UpdateMutation(task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(),
                task.getTaskInfo().getPartitionName(), task.getTaskInfo().getPartitions(),
                config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            Path path = new Path(task.getTaskInfo().getFilePath());
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if (!fileSystem.exists(path)) {
                LOG.warn("path not exist: {}", task.getTaskInfo().getFilePath());
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
                return;
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();
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
            MetricReporter.updatedBytes(bytes);
            long endTime = System.currentTimeMillis();
            LOG.info("mysql file task success: {} cost: {}ms", task.getTaskInfo().getFilePath(), endTime - startTime);

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

    public void process(ConsumedTask consumedTask) {
        this.taskQueue.offer(consumedTask);
    }

    public void stop() {
        this.stop = true;
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
                    continue;
                }
                Long bytes = fileSystem.getFileStatus(path).getLen();
                try (AvroFileReader reader = new AvroFileReader(path);) {
                    Schema recordSchema = reader.getSchema();
                    updateMutation.beginStreamTransaction(recordSchema, ConfigHolder.getHiveConf());
                    while (reader.hasNext()) {
                        GenericData.Record record = reader.next();
                        updateMutation.update(record, false);
                    }
                }
                updateMutation.commitTransaction();
                dbTaskStatusListener.onTaskSuccess(task);
                MetricReporter.updatedBytes(bytes);
                long endTime = System.currentTimeMillis();
                LOG.info("additional file task success: {} cost: {}ms", task.getFilePath(), endTime - startTime);

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
            } catch (DataDelayedException e) {
                LOG.info("additional task delayed: {}", task);
                dbTaskStatusListener.onTaskDelay(task);
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
}
