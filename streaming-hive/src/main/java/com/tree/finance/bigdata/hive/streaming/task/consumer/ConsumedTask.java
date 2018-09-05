package com.tree.finance.bigdata.hive.streaming.task.consumer;

import com.tree.finance.bigdata.task.TaskInfo;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 10:07
 */
public interface ConsumedTask {
    TaskInfo getTaskInfo();
    void taskRejected();
}
