package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.task.consumer.RabbitMqTask;
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
        //no opot
    }

    @Override
    public void onTaskSuccess(List<RabbitMqTask> tasks) {
        for (RabbitMqTask task : tasks) {
            onTaskSuccess(task);
        }
    }

    @Override
    public void onTaskRetry(List<RabbitMqTask> tasks) {
        // no pot
    }
}
