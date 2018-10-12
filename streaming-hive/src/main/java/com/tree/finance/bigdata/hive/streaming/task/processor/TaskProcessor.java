package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.task.listener.DbTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.SPACE;
import static com.tree.finance.bigdata.hive.streaming.config.Constants.SQL_VALUE_QUOTE;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 19:31
 */
public abstract class TaskProcessor {

    protected AppConfig config;

    protected ConnectionFactory factory;

    private Logger LOG = LoggerFactory.getLogger(TaskProcessor.class);

    protected MqTaskStatusListener mqTaskStatusListener;

    protected DbTaskStatusListener dbTaskStatusListener;

    TaskProcessor(AppConfig config, ConnectionFactory factory){
        this.config = config;
        this.factory = factory;
        this.mqTaskStatusListener = new MqTaskStatusListener();
        this.dbTaskStatusListener = new DbTaskStatusListener(factory);
    }

    protected abstract void handleMoreTask(TaskInfo task);

    protected List<TaskInfo> getSameTask(TaskInfo taskInfo) {
        List<TaskInfo> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder("select id, file_path, attempt from ")
                .append(config.getTaskTleName()).append(SPACE)
                .append("where db=").append(SQL_VALUE_QUOTE).append(taskInfo.getDb()).append(SQL_VALUE_QUOTE).append(" and ")
                .append("table_name =").append(SQL_VALUE_QUOTE).append(taskInfo.getTbl()).append(SQL_VALUE_QUOTE).append(" and ")
                .append("partition_name =").append(SQL_VALUE_QUOTE).append(taskInfo.getPartitionName()).append(SQL_VALUE_QUOTE).append("and ")
                .append("op =").append(SQL_VALUE_QUOTE).append(taskInfo.getOp().code()).append(SQL_VALUE_QUOTE).append(" and ")
                .append(" status=").append(SQL_VALUE_QUOTE).append(TaskStatus.NEW).append(SQL_VALUE_QUOTE).append(" and ")
                .append(" id !=").append(SQL_VALUE_QUOTE).append(taskInfo.getId()).append(SQL_VALUE_QUOTE).append(" and ")
                .append(" file_path !=").append(SQL_VALUE_QUOTE).append(taskInfo.getFilePath()).append(SQL_VALUE_QUOTE)
                .append(" order by id asc");
        try (Connection conn = factory.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sb.toString())) {
            while (rs.next()) {
                list.add(new TaskInfo(rs.getString(1), taskInfo.getDb(), taskInfo.getTbl()
                        , taskInfo.getPartitions(), taskInfo.getPartitionName(), rs.getString(2), taskInfo.getOp(),
                        rs.getInt(3)));
            }
            return list;
        } catch (Exception e) {
            LOG.error("failed to get more task, query sql: {}", sb.toString(), e);
            return list;
        }
    }

}
