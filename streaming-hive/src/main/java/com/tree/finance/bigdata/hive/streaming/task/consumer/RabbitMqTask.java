package com.tree.finance.bigdata.hive.streaming.task.consumer;

import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.utils.mq.ChannelMsg;
import com.tree.finance.bigdata.task.TaskInfo;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 10:05
 */
public class RabbitMqTask implements ConsumedTask{

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
        new MqTaskStatusListener(ConfigHolder.getConfig()).onTaskError(this);
    }

    public ChannelMsg getChannelMsg() {
        return channelMsg;
    }
}
