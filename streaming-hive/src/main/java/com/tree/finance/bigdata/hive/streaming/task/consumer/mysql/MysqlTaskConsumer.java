package com.tree.finance.bigdata.hive.streaming.task.consumer.mysql;

import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.consumer.TaskConsumer;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/13 18:44
 */
public class MysqlTaskConsumer implements TaskConsumer<MysqlTask> {

    private static Logger LOG = LoggerFactory.getLogger(MysqlTaskConsumer.class);
    private static final String SEP = "'";
    private Integer maxRetries;
    private Integer delayTaskHours;

    public MysqlTaskConsumer() {
        this.maxRetries = ConfigHolder.getConfig().getDelayTaskMaxRetries();
        this.delayTaskHours = ConfigHolder.getConfig().getDelayTaskHours();
    }

    /*public List<MysqlTask> consumeBatch() {
        List<MysqlTask> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder("select ID, DB, TABLE_NAME, PARTITION_NAME, FILE_PATH, OP, ATTEMPT from ")
                .append(ConfigHolder.getConfig().getTaskTleName())
                .append(" where STATUS = ").append(SEP).append(TaskStatus.DELAY).append(SEP)
                .append(" and attempt <= ").append(maxRetries)
                .append(" order by ID asc");
        try (Connection conn = ConfigHolder.getDbFactory().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sb.toString())) {
            while (rs.next()) {
                String id = rs.getString(1);
                String db = rs.getString(2);
                String table = rs.getString(3);
                String parName = rs.getString(4);
                List<String> partitions = getPartition(parName);
                String filePath = rs.getString(5);
                Operation op = Operation.forCode(rs.getString(6));
                Integer attempt = rs.getInt(7);
                result.add(new MysqlTask(new TaskInfo(id, db, table, partitions, parName, filePath, op, attempt)));
            }
        } catch (Exception e) {
            LOG.error("failed to execute: {}", sb.toString(), e);
            return null;
        }
        return result;
    }*/

    public List<MysqlTask> consumeBatch() {
        List<MysqlTask> result = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, 0 - delayTaskHours);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuilder sb = new StringBuilder("select ID, DB, TABLE_NAME, PARTITION_NAME, FILE_PATH, OP, ATTEMPT from ")
                .append(ConfigHolder.getConfig().getTaskTleName())
                .append(" where STATUS = ").append(SEP).append(TaskStatus.NEW).append(SEP)
                .append(" and CREATE_TIME <= ").append("TIMESTAMP(").append(sdf.format(calendar.getTime()))
                .append(" and attempt <= ").append(maxRetries)
                .append(" order by ID asc");
        try (Connection conn = ConfigHolder.getDbFactory().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sb.toString())) {
            while (rs.next()) {
                String id = rs.getString(1);
                String db = rs.getString(2);
                String table = rs.getString(3);
                String parName = rs.getString(4);
                List<String> partitions = getPartition(parName);
                String filePath = rs.getString(5);
                Operation op = Operation.forCode(rs.getString(6));
                Integer attempt = rs.getInt(7);
                result.add(new MysqlTask(new TaskInfo(id, db, table, partitions, parName, filePath, op, attempt)));
            }
        } catch (Exception e) {
            LOG.error("failed to execute: {}", sb.toString(), e);
            return null;
        }
        return result;
    }

    /*
     * @parName: p_y=2018/p_m=9/p_d=8
     */
    public static List<String> getPartition(String parName) {
        List<String> result = new ArrayList<>();
        for (String kv : parName.split("/")) {
            String[] kvs = kv.split("=");
            result.add(kvs[1]);
        }
        return result;
    }

    @Override
    public MysqlTask consume() throws IOException {
        return null;
    }

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }
}
