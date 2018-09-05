package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.consumer.RabbitMqTask;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
            TaskStatusListener.doWithTaskFile(rabbitMqTask.getTaskInfo().getFilePath());
            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);
        } catch (Exception e) {
            //todo alarm avoid duplication
            LOG.error("error ack msg for task: {}\n{}", rabbitMqTask.getTaskInfo(), e);
        }
    }

    @Override
    public void onTaskError(RabbitMqTask rabbitMqTask) {
        try {
            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);
        } catch (Exception e) {
            //todo alarm retry or something
            LOG.error("may cause data inaccuracy: {}", rabbitMqTask.getTaskInfo(), e);
        }
    }

    @Override
    public void onTaskDelay(RabbitMqTask rabbitMqTask) {
        try {
            rabbitMqTask.getChannelMsg().getChannel().basicAck(rabbitMqTask.getChannelMsg().getMsg().getEnvelope().getDeliveryTag(),
                    false);
            LOG.info("delete delayed task info, leave it in database");
        } catch (Exception e) {
            LOG.error("will not cause data inaccuracy", e);
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
