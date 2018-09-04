package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.mysql.cj.x.protobuf.MysqlxCrud;
import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.TaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.utils.InsertMutation;
import com.tree.finance.bigdata.hive.streaming.utils.Mutation;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

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
        String db = tasks.get(0).getTaskInfo().getDb();
        String table = tasks.get(0).getTaskInfo().getTbl();
        String partitionName = tasks.get(0).getTaskInfo().getPartitionName();
        List<String> partitions = tasks.get(0).getTaskInfo().getPartitions();

        LOG.info("start batch tasks: [{}.{}.{}], task size: {}", db, table
                , partitionName, tasks.size());

        InsertMutation mutationUtils = new InsertMutation(db, table, partitionName, partitions,
                ConfigHolder.getConfig().getMetastoreUris(), ConfigHolder.getHbaseConf());

        try {
            int i = 0;

            for (ConsumedTask task : tasks) {
                i++;
                long startTime = System.currentTimeMillis();
                LOG.info("batch file task {} start: {}", i, task.getTaskInfo().getFilePath());
                handleTask(mutationUtils, task);
                LOG.info("batch file task {} success: {} cost: {}ms", i, task.getTaskInfo().getFilePath()
                        , System.currentTimeMillis() - startTime);
            }
            mutationUtils.commitTransaction();
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
                    mutationUtils.abortTxn();
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
            mutationUtils.abortTxn();
            for (ConsumedTask task : tasks) {
                handleByEach(task, 2);
            }
        }
    }

    private void handleByEach(ConsumedTask task, int remainningRetry) {

        InsertMutation mutationUtils = new InsertMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigHolder.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("single task start: {}", task.getTaskInfo().getFilePath());
            handleTask(mutationUtils, task);
            LOG.info("single task success: {}, cost: {}ms", task.getTaskInfo().getFilePath(), System.currentTimeMillis() - start);
            mutationUtils.commitTransaction();
            try {
                taskStatusListener.onTaskSuccess(task);
            } catch (Exception e) {
                // ignore ack failure. Cause once success, source file is renamed, and will not be retried
                LOG.error("ack success failed, will not affect data accuracy", e);
            }

        } catch (TransactionException e) { // if get transaction exception retry batch, or send to retry queue
            mutationUtils.abortTxn();
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
                mutationUtils.abortTxn();
                taskStatusListener.onTaskError(task);
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }


    private void handleTask(InsertMutation mutationUtils, ConsumedTask task) throws Exception {

        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path path = new Path(task.getTaskInfo().getFilePath());

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
                mutationUtils.beginTransaction(recordSchema);
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                mutationUtils.insert(record, false);
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
