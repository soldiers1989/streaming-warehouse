package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.TaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.utils.UpdateMutation;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import io.prometheus.client.Summary;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/17 10:28
 */
public class UpdateTaskProcessor implements Runnable {

    private AppConfig config;

    private static TaskStatusListener taskStatusListener;

    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    UpdateTaskProcessor(AppConfig config, int id) {
        this.config = config;
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.taskStatusListener = new MqTaskStatusListener(config);
        this.thread = new Thread(this::run, "Update-Processor-" + id);
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
                config.getMetastoreUris(), ConfigHolder.getHbaseConf());

        try {
            Path path = new Path(task.getTaskInfo().getFilePath());
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if (!fileSystem.exists(path)) {
                LOG.warn("path not exist: {}", task.getTaskInfo().getFilePath());
                taskStatusListener.onTaskSuccess(task);
                return;
            }
            Long bytes = fileSystem.getFileStatus(path).getLen();

            AvroFileReader reader = new AvroFileReader(path);
            Schema recordSchema = reader.getSchema();
            updateMutation.beginTransaction(recordSchema);

            //recordId, 降序排列
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                updateMutation.update(record, false);
            }
            updateMutation.commitTransaction();

            taskStatusListener.onTaskSuccess(task);

            MetricReporter.updatedBytes(bytes);

            long endTime = System.currentTimeMillis();
            LOG.info("file task success: {} cost: {}ms", task.getTaskInfo().getFilePath(), endTime - startTime);

        } catch (DataDelayedException e) {
            taskStatusListener.onTaskDelay(task);
        } catch (Throwable t) {
            LOG.error("file task failed: " + task.getTaskInfo().getFilePath(), t);
            try {
                updateMutation.abortTxn();
                taskStatusListener.onTaskError(task);
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

}
