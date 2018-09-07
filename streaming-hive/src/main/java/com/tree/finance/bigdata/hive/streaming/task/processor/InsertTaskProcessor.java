package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.RabbitMqTask;
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

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

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
                ConsumedTask task = taskQueue.take();
                handleByEach(task);
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    private void handleByEach(ConsumedTask task) {
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
            LOG.warn("txn exception", e);
            mutationUtils.abortTxn();
            // not update info in database, but ack mq message
            mqTaskStatusListener.onTaskSuccess((RabbitMqTask) task);
        } catch (Throwable t) {
            LOG.error("file task failed: {}", task.getTaskInfo(), t);
            try {
                mutationUtils.abortTxn();
                mqTaskStatusListener.onTaskError((RabbitMqTask) task);
                dbTaskStatusListener.onTaskError(task.getTaskInfo());
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }

    protected void handleMoreTask(TaskInfo task) {
        List<TaskInfo> moreTasks;
        int greedyCount = config.getGreedyProcessBatchLimit();
        TaskInfo errorTask = null;
        while (!(moreTasks = getSameTask(task)).isEmpty()) {
            InsertMutation mutationUtils = new InsertMutation(task.getDb(),
                    task.getTbl(), task.getPartitionName(),
                    task.getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
            try {
                LOG.info("going to process {} more tasks", moreTasks.size());
                List<TaskInfo> processed = new ArrayList<>();
                for (TaskInfo sameTask : moreTasks) {
                    if (processed.size() >= greedyCount) {
                        mutationUtils.commitTransaction();
                        LOG.info("processed {} tasks, committed as a batch", processed.size());
                        dbTaskStatusListener.onTaskSuccess(processed);
                        mutationUtils = new InsertMutation(task.getDb(),
                                task.getTbl(), task.getPartitionName(),
                                task.getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
                        processed.clear();
                    }
                    try {
                        handleTask(mutationUtils, sameTask);
                        LOG.info("file task success in batch: {}", sameTask.getFilePath());
                    } catch (Exception e) {
                        errorTask = sameTask;
                        LOG.error("failed to process task: {}", errorTask, e);
                        throw new RuntimeException("task failed in batch");
                    }
                    processed.add(sameTask);
                }
                if (!processed.isEmpty()) {
                    mutationUtils.commitTransaction();
                    LOG.info("processed {} tasks, committed as a batch", processed.size());
                    dbTaskStatusListener.onTaskSuccess(processed);
                }
                LOG.info("finished processing {} more tasks", moreTasks.size());
            } catch (TransactionException e) {
                mutationUtils.abortTxn();
                // if get transaction exception ignore it, let others process this task
                LOG.warn("ignore this transaction exception， let others process this task", e);
            } catch (Throwable t) { //if other error try batch by single
                LOG.error("file task failed, {}", task);
                mutationUtils.abortTxn();
                dbTaskStatusListener.onTaskError(errorTask);
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
                mutationUtils.beginStreamTransaction(recordSchema);
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                mutationUtils.insert(record);
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
