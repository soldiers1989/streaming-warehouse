package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.type.RabbitMqTask;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.FILE_PROCESSED_SUFIX;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/10 20:24
 */
public class MqTaskStatusListener implements TaskStatusListener<RabbitMqTask> {

    private AppConfig config;

    public MqTaskStatusListener(AppConfig config) {
        this.config = config;
    }

    private static Logger LOG = LoggerFactory.getLogger(MqTaskStatusListener.class);

    @Override
    public void onTaskSuccess(RabbitMqTask rabbitMqTask) {
        try {

            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);

            if (ConfigHolder.getConfig().deleteAvroOnSuccess()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                Path path = new Path(rabbitMqTask.getTaskInfo().getFilePath());
                if (fs.exists(path)) {
                    fs.delete(path, true);
                }
            } else {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                Path newPath = new Path(rabbitMqTask.getTaskInfo().getFilePath() + FILE_PROCESSED_SUFIX);
                fs.rename(new Path(rabbitMqTask.getTaskInfo().getFilePath()), newPath);
            }

        } catch (Exception e) {
            //todo alarm avoid duplication
            LOG.error("error ack msg for task: {}\n{}", rabbitMqTask.getTaskInfo(), e);
        }
    }

    @Override
    public void onTaskError(RabbitMqTask rabbitMqTask) {
        try {
            TaskInfo retryTaskInfo = rabbitMqTask.getTaskInfo();
            Integer retryCount = retryTaskInfo.getRetryCount();

            //todo use mq transaction
            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);

            LOG.info("delete previous retry task info");
            RabbitMqUtils rabbitMqUtils = RabbitMqUtils.getInstance(ConfigHolder.getConfig().getRabbitHost(), ConfigHolder.getConfig().getRabbitPort());

            //update attempt info and send to error queue
            retryTaskInfo.setRetryCount(++retryCount);
            retryTaskInfo.setLastUpdateTime(System.currentTimeMillis());
            rabbitMqUtils.produce(ConfigHolder.getConfig().getRabbitErrorQueueName(),
                    JSON.toJSONString(retryTaskInfo));
            LOG.info("updated retry task info: {}", retryTaskInfo);

        } catch (Exception e) {
            //todo alarm retry or something
            LOG.error("may cause data inaccuracy: {}", rabbitMqTask.getTaskInfo(), e);
        }
    }

    @Override
    public void onTaskDelay(RabbitMqTask rabbitMqTask) {
        try {
            RabbitMqUtils rabbitMqUtils = RabbitMqUtils.getInstance(config.getRabbitHost(), config.getRabbitPort());
            TaskInfo taskInfo = rabbitMqTask.getTaskInfo();

            //update attempt info and send to delay queue
            Integer retry = taskInfo.getRetryCount();
            taskInfo.setRetryCount(++retry);
            taskInfo.setLastUpdateTime(System.currentTimeMillis());
            rabbitMqUtils.produce(ConfigHolder.getConfig().getRabbitDelayQueueName(),
                    JSON.toJSONString(taskInfo));
            LOG.info("delayed task was sent to delay queue: {}", taskInfo);

            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);
            LOG.info("delete previous task info");

        } catch (Exception e) {
            LOG.error("may cause data inaccuracy", e);
        }
    }

    @Override
    public void onTaskRetry(RabbitMqTask rabbitMqTask) {
        try {

            TaskInfo taskInfo = rabbitMqTask.getTaskInfo();
            Integer maxRetries = config.getTaskRetriesOnError();
            Integer retry = taskInfo.getRetryCount();
            if (retry >= maxRetries) {
                LOG.info("exceed max retry : {}", taskInfo);
                onTaskError(rabbitMqTask);
            } else {
                RabbitMqUtils rabbitMqUtils = RabbitMqUtils.getInstance(config.getRabbitHost(), config.getRabbitPort());
                //update attempt info and send to task queue, to retry it
                taskInfo.setRetryCount(++retry);
                taskInfo.setLastUpdateTime(System.currentTimeMillis());
                rabbitMqUtils.produce(ConfigHolder.getConfig().getRabbitDelayQueueName(),
                        JSON.toJSONString(taskInfo));
                LOG.info("retry task was sent to task queue: {}", taskInfo);
                //delete previous task info
                rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                        false);
                LOG.info("delete previous task info");
            }
        } catch (Exception e) {
            LOG.error("may cause data inaccuracy", e);
        }
    }

    @Override
    public void onTaskSuccess(List<RabbitMqTask> tasks) {
        for (RabbitMqTask task : tasks) {
            onTaskSuccess(task);
        }
    }

    @Override
    public void onTaskRetry(List<RabbitMqTask> tasks) {
        for (RabbitMqTask task : tasks) {
            onTaskRetry(task);
        }
    }
}
