package com.tree.finance.bigdata.hive.streaming.task.listener;

import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.SQL_VALUE_QUOTE;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 17:18
 */
public class DbTaskStatusListener implements TaskStatusListener<TaskInfo> {

    private ConnectionFactory factory;

    private Logger LOG = LoggerFactory.getLogger(DbTaskStatusListener.class);

    public DbTaskStatusListener(ConnectionFactory factory) {
        this.factory = factory;
    }

    @Override
    public void onTaskSuccess(TaskInfo taskInfo) {
        TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement()) {
            StringBuilder sb = new StringBuilder("update ")
                    .append(ConfigHolder.getConfig().getTaskTleName())
                    .append(" set status = ").append(SQL_VALUE_QUOTE).append(TaskStatus.SUCCESS).append(SQL_VALUE_QUOTE)
                    .append(" where id= ").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE);
            TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
            stmt.executeUpdate(sb.toString());
        } catch (Exception e) {
            LOG.error("may cause data inaccuracy", e);
        }
    }

    @Override
    public void onTaskError(TaskInfo taskInfo) {
        if (null == taskInfo) {
            return;
        }
        TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement()) {
            StringBuilder sb = new StringBuilder("update ")
                    .append(ConfigHolder.getConfig().getTaskTleName())
                    .append(" set status = ").append(SQL_VALUE_QUOTE).append(TaskStatus.FAIL).append(SQL_VALUE_QUOTE)
                    .append(" where id= ").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE);
            TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
            stmt.executeUpdate(sb.toString());
        } catch (Exception e) {
            LOG.error("failed to update failed task", e);
        }
    }

    @Override
    public void onTaskDelay(TaskInfo taskInfo) {
        if (null == taskInfo) {
            return;
        }
        StringBuilder sb = new StringBuilder("update ")
                .append(ConfigHolder.getConfig().getTaskTleName())
                .append(" set status = ").append(SQL_VALUE_QUOTE).append(TaskStatus.DELAY).append(SQL_VALUE_QUOTE)
                .append(" where id= ").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE);
        TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sb.toString());
        } catch (Exception e) {
            LOG.error("failed to execute {}", sb.toString());
        }
    }

    @Override
    public void onTaskRetry(TaskInfo taskInfo) {
        //no opt
    }

    @Override
    public void onTaskSuccess(List<TaskInfo> tasks) {
        for (TaskInfo taskInfo : tasks) {
            TaskStatusListener.doWithTaskFile(taskInfo.getFilePath());
        }
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement()) {
            for (TaskInfo taskInfo : tasks) {
                StringBuilder sb = new StringBuilder("update ")
                        .append(ConfigHolder.getConfig().getTaskTleName())
                        .append(" set status = ").append(SQL_VALUE_QUOTE).append(TaskStatus.SUCCESS).append(SQL_VALUE_QUOTE)
                        .append(" where id= ").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE);
                stmt.addBatch(sb.toString());
            }
            stmt.executeBatch();
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    @Override
    public void onTaskRetry(List<TaskInfo> tasks) {
        //will not retry
    }

}
