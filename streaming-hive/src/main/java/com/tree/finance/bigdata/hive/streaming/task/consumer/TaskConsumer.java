package com.tree.finance.bigdata.hive.streaming.task.consumer;

import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 20:18
 */
public interface TaskConsumer<T extends ConsumedTask> {
    T consume() throws IOException;
    void init();
    void close();
}
