package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.task.TaskInfo;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:10
 */
public interface Service {

    void stop() throws InterruptedException;

    void init() throws Exception;

}
