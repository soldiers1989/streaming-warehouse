package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.task.TaskInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.FILE_ERROR_SUFFIX;
import static com.tree.finance.bigdata.hive.streaming.config.Constants.FILE_PROCESSED_SUFFIX;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/10 20:30
 */
public interface TaskStatusListener<T> {

    Logger LOG = LoggerFactory.getLogger(TaskStatusListener.class);

    String backUpPath = ConfigHolder.getConfig().getIntermediateBackUpPath();

    String PATH_SEP = "/";

    void onTaskSuccess(T t);

    void onTaskError(T t);

    void onTaskDelay(T t);

    void onTaskRetry(T t);

    void onTaskSuccess(List<T> tasks);

    void onTaskRetry(List<T> tasks);

    static void taskFileOnSuccess(TaskInfo taskInfo) {
        String filePath = taskInfo.getFilePath();
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
                if (!fs.exists(new Path(filePath))) {
                    return;
                }

                DateTime dateTime = DateTime.now();
                String currentDate = dateTime.toString("yyyy/MM/dd");

                Path original = new Path(taskInfo.getFilePath());
                StringBuilder sb = new StringBuilder(backUpPath)
                        .append(taskInfo.getOp().name()).append(PATH_SEP)
                        .append(currentDate).append(PATH_SEP)
                        .append(taskInfo.getDb()).append(PATH_SEP)
                        .append(taskInfo.getTbl()).append(PATH_SEP)
                        .append(original.getName()).append(FILE_PROCESSED_SUFFIX);

                Path newPath = new Path(sb.toString());

                if (!fs.exists(newPath.getParent())){
                    fs.mkdirs(newPath.getParent());
                }

                if (!fs.rename(original, newPath)){
                    LOG.warn("may cause data inaccuracy, failed to rename processed file: {} to {}",
                            filePath, newPath);
                }
            }
        } catch (Exception e) {
            LOG.error("may cause data inaccuracy, failed to rename or delete processed file: {}", filePath, e);
        }
    }

    static void taskFileOnError(String filePath) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(new Path(filePath))) {
                return;
            }
            Path newPath = new Path(filePath + FILE_ERROR_SUFFIX);
            fs.rename(new Path(filePath), newPath);
        } catch (Exception e) {
            LOG.error("may cause data inaccuracy, failed to rename or delete error file: {}", filePath, e);
        }
    }
}
