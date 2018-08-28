package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/10 20:30
 */
public interface TaskStatusListener<T extends ConsumedTask> {
    void onTaskSuccess(T t);
    void onTaskError(T t);
    void onTaskDelay(T t);
    void onTaskRetry(T t);

    void onTaskSuccess(List<T> tasks);
    void onTaskRetry(List<T> tasks);
}
