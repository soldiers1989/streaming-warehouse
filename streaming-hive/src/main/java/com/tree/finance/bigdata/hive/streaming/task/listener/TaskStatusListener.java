package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
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
 * Created in 2018/7/10 20:30
 */
public interface TaskStatusListener<T> {

    Logger LOG = LoggerFactory.getLogger(TaskStatusListener.class);

    void onTaskSuccess(T t);

    void onTaskError(T t);

    void onTaskDelay(T t);

    void onTaskRetry(T t);

    void onTaskSuccess(List<T> tasks);

    void onTaskRetry(List<T> tasks);

    static void doWithTaskFile(String filePath) {
        try {
            if (ConfigHolder.getConfig().deleteAvroOnSuccess()) {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                Path path = new Path(filePath);
                if (fs.exists(path)) {
                    fs.delete(path, true);
                }
            } else {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);
                if (!fs.exists(new Path(filePath))){
                    return;
                }
                Path newPath = new Path(filePath + FILE_PROCESSED_SUFIX);
                fs.rename(new Path(filePath), newPath);
            }
        } catch (Exception e) {
            LOG.error("may cause data inaccuracy, failed to rename or delete processed file: {}", filePath);
        }
    }
}
