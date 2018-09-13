package com.tree.finance.bigdata.hive.streaming.task.consumer.mysql;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.Statement;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/13 18:47
 */
public class MysqlTask implements ConsumedTask {

    private TaskInfo taskInfo;

    private static Logger LOG;

    public MysqlTask(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public void taskRejected() {
        try (Connection conn = ConfigHolder.getDbFactory().getConnection();
             Statement stmt = conn.createStatement()) {
            String s = "update %s set STATUS = '%s' and id= '%s'";
            stmt.execute(String.format(s, ConfigHolder.getConfig().getTaskTleName(),
                    taskInfo.getId(), TaskStatus.FAIL.name()));
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}
