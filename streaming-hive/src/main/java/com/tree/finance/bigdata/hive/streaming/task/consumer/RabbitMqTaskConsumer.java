package com.tree.finance.bigdata.hive.streaming.task.consumer;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.utils.mq.ChannelMsg;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import com.tree.finance.bigdata.task.TaskInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 20:20
 */
public class RabbitMqTaskConsumer implements TaskConsumer<RabbitMqTask> {

    private RabbitMqUtils rabbitMqUtils;

    private static Logger LOG = LoggerFactory.getLogger(RabbitMqTaskConsumer.class);

    private String taskQueue;

    private AppConfig conf;

    public RabbitMqTaskConsumer(AppConfig conf) {
        this.conf = conf;
        this.taskQueue = conf.getRabbitQueueName();
        this.rabbitMqUtils = RabbitMqUtils.getInstance(conf.getRabbitHost(), conf.getRabbitPort());
    }

    public void init() {
    }

    @Override
    public RabbitMqTask consume() throws IOException {

        TaskInfo taskInfo;
        ChannelMsg channelMsg = null;
        try {
            channelMsg = rabbitMqUtils.consume(taskQueue, conf.getRabbitQos());
            taskInfo = JSON.parseObject(channelMsg.getMsg().getBody(), TaskInfo.class);
        } catch (InterruptedException e) {
            LOG.info(Thread.currentThread().getName() + " interrupted");
            Thread.currentThread().interrupt();
            return null;
        }
        catch (Exception e) {
            if (null != channelMsg) {
                LOG.error("error parsing task, will not retry. task msg: " + new String(channelMsg.getMsg().getBody()), e);
                channelMsg.getChannel().basicAck(channelMsg.getMsg().getEnvelope().getDeliveryTag(), false);
            }
            LOG.error("error consume task", e);
            return null;
        }
        return new RabbitMqTask(channelMsg, taskInfo);
    }

    @Override
    public void close() {
        //no opt, rabbitMqUtils should not close here
    }

}
