package com.tree.finance.bigdata.hive.streaming.task.consumer.mq;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.utils.mq.ChannelMsg;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 10:05
 */
public class RabbitMqTask implements ConsumedTask {

    private TaskInfo taskInfo;

    private ChannelMsg channelMsg;

    public RabbitMqTask(ChannelMsg channelMsg, TaskInfo taskInfo){
        this.taskInfo = taskInfo;
        this.channelMsg = channelMsg;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public void taskRejected() {
        new MqTaskStatusListener().onTaskError(this);
    }

    public ChannelMsg getChannelMsg() {
        return channelMsg;
    }
}
