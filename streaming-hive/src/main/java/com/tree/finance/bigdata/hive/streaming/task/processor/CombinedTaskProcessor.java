package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.utils.CombinedMutation;
import com.tree.finance.bigdata.hive.streaming.utils.MutateResult;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
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

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.SPACE;
import static com.tree.finance.bigdata.hive.streaming.config.Constants.SQL_VALUE_QUOTE;

public class CombinedTaskProcessor extends TaskProcessor{
    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private LinkedBlockingQueue<RabbitMqTask> taskQueue;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    public CombinedTaskProcessor(AppConfig config, ConnectionFactory factory, int id) {
        super(config, factory);
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.thread = new Thread(this::run, "Combine-Processor-" + id);
    }

    public void init() {
        thread.start();
    }

    public void run() {
        while (!stop || !taskQueue.isEmpty()) {
            try {
                RabbitMqTask task = taskQueue.take();
                if (handleWithMqTask(task)) {    //if successfuly handled current task, try to handle as more as possible
                    while (!CollectionUtils.isEmpty(handleGreedyDbTasks(task))) {
                        LOG.info("handled greedy tasks successfully, going to handle another greedy batch");
                    }
                }
            } catch (InterruptedException e) {
                //no opt
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    public List<TaskInfo> handleGreedyDbTasks(RabbitMqTask rabbitMqTask) {
        List<TaskInfo> greedyTasks = getSameTask(rabbitMqTask.getTaskInfo());
        return handleDbBatchTask(greedyTasks);
    }

    private List<TaskInfo> handleDbBatchTask(List<TaskInfo> batchTasks) {

        if (CollectionUtils.isEmpty(batchTasks)) {
            return null;
        }

        String db = batchTasks.get(0).getDb();
        String table = batchTasks.get(0).getTbl();

        CombinedMutation mutationUtils = new CombinedMutation(db,
                table, batchTasks.get(0).getPartitionName(),
                batchTasks.get(0).getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        TaskInfo processing;
        try {
            if (CollectionUtils.isEmpty(batchTasks)) {
                LOG.info("Batch task is empty");
                return null;
            }
            long batchStart = System.currentTimeMillis();
            LOG.info("going to process {} more tasks", batchTasks.size());
            long start;
            for (TaskInfo task : batchTasks) {
                processing = task;
                start = System.currentTimeMillis();
                handleTask(mutationUtils, task);
                LOG.info("additional task success in greedy batch: {}, cost: {}", task.getFilePath(), System.currentTimeMillis() - start);
            }
            MutateResult result  = mutationUtils.commitTransaction();
            try {
                dbTaskStatusListener.onTaskSuccess(batchTasks);
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.warn("ack success failed, will not affect repair accuracy", e);
            }
            MetricReporter.wroteRecords(result, db, table);
            MetricReporter.processedFiles(batchTasks.size());
            return batchTasks;
        } catch (TransactionException e) {
            try {
                if (e.getCause() instanceof LockException) {
                    LOG.warn("failed to lock for greedy tasks, txnId: {}", mutationUtils.getTransactionId());
                } else {
                    LOG.error("txn exception", e);
                }
                mutationUtils.abortTxn();
            } catch (Exception ex) {
                LOG.error("error handle transaction exception", ex);
            }
        } catch (Throwable t) {
            try {
                mutationUtils.abortTxn();
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
        return null;
    }

    /**
     * @param task
     * @return true: successfully handled mq task and more tasks
     */
    private boolean handleWithMqTask(RabbitMqTask task) {
        CombinedMutation mutationUtils = new CombinedMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("single task start: {}", task.getTaskInfo().getFilePath());
            boolean success = handleTask(mutationUtils, task.getTaskInfo());
            if (!success) {
                mqTaskStatusListener.onTaskSuccess(task);
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
                return false;
            }

            LOG.info("single task success: {}, cost: {}ms", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);
            long batchStart = System.currentTimeMillis();

            TaskInfo previousTaskInfo = task.getTaskInfo();
            List<TaskInfo> moreTasks = getSameTask(previousTaskInfo);
            if (!CollectionUtils.isEmpty(moreTasks)) {
                LOG.info("going to process {} more tasks", moreTasks.size());
                for (TaskInfo sameTask : moreTasks) {
                    start = System.currentTimeMillis();
                    handleTask(mutationUtils, sameTask);
                    LOG.info("additional task success in batch: {}, cost: {}", sameTask.getFilePath(), System.currentTimeMillis() - start);
                }
            }
            MutateResult result = mutationUtils.commitTransaction();
            try {
                mqTaskStatusListener.onTaskSuccess(task);
                moreTasks.add(task.getTaskInfo());
                dbTaskStatusListener.onTaskSuccess(moreTasks);
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.warn("ack success failed, will not affect repair accuracy", e);
            }
            MetricReporter.wroteRecords(result, task.getTaskInfo().getDb(), task.getTaskInfo().getTbl());
            MetricReporter.processedFiles(moreTasks.size());
            return true;
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
            } finally {
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
        return false;
    }

    /**
     * @param mutationUtils
     * @param task
     * @return success: true
     * @throws Exception
     */
    private boolean handleTask(CombinedMutation mutationUtils, TaskInfo task) throws Exception {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(task.getFilePath());
        if (!fileSystem.exists(path)) {
            LOG.info("path not exist: {}", task.getFilePath());
            return false;
        }
        try (AvroFileReader reader = new AvroFileReader(new Path(task.getFilePath()))) {
            if (!mutationUtils.txnOpen()) {
                Schema recordSchema = reader.getSchema();
                //check insert record exist
                mutationUtils.beginTransaction(recordSchema, ConfigHolder.getHiveConf());
            }
            long insertStart = System.currentTimeMillis();
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                mutationUtils.mutate(record);
            }
            LOG.info("insert task in batch cost: {}", System.currentTimeMillis() - insertStart);
        }catch (FileNotFoundException e) {
            //ignore
            return false;
        }
        return true;
    }

    @Override
    protected List<TaskInfo> getSameTask(TaskInfo taskInfo) {
        long start = System.currentTimeMillis();
        List<TaskInfo> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder("select id, file_path, attempt from ")
                .append(config.getTaskTleName()).append(SPACE)
                .append("where db=").append(SQL_VALUE_QUOTE).append(taskInfo.getDb()).append(SQL_VALUE_QUOTE).append(" and ")
                .append("table_name =").append(SQL_VALUE_QUOTE).append(taskInfo.getTbl()).append(SQL_VALUE_QUOTE).append(" and ")
                .append("partition_name =").append(SQL_VALUE_QUOTE).append(taskInfo.getPartitionName()).append(SQL_VALUE_QUOTE).append("and ")
                .append(" status=").append(SQL_VALUE_QUOTE).append(TaskStatus.NEW).append(SQL_VALUE_QUOTE).append(" and ")
                .append(" id !=").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE).append(" and ")
                .append(" file_path !=").append(SQL_VALUE_QUOTE).append(taskInfo.getFilePath()).append(SQL_VALUE_QUOTE)
                .append(" limit ")
                .append(config.getGreedyProcessBatchLimit());
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sb.toString())) {
            while (rs.next()) {
                list.add(new TaskInfo(rs.getString(1), taskInfo.getDb(), taskInfo.getTbl()
                        , taskInfo.getPartitions(), taskInfo.getPartitionName(), rs.getString(2), taskInfo.getOp(),
                        rs.getInt(3)));
            }
            LOG.info("get more task cost: {}", System.currentTimeMillis() - start);
            return list;
        } catch (Exception e) {
            LOG.info("get more task cost: {}", System.currentTimeMillis() - start);
            LOG.error("failed to get more task, query sql: {}", sb.toString(), e);
            return list;
        }
    }

    public boolean commit(RabbitMqTask consumedTask) {
        try {
            return this.taskQueue.offer(consumedTask, 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("queue is full: {}", id);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void stop() {
        this.stop = true;
        this.thread.interrupt();
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping Combine-Processor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped Combine-Processor-{}", id);
    }
}
