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
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/13 18:44
 */
public class MysqlTaskConsumer implements TaskConsumer<MysqlTask> {

    private static Logger LOG = LoggerFactory.getLogger(MysqlTaskConsumer.class);
    private static final String SEP = "'";

    public List<MysqlTask> consumeBatch() {
        List<MysqlTask> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder("select ID, DB, TABLE_NAME, PARTITION_NAME, FILE_PATH, OP from ")
                .append(ConfigHolder.getConfig().getTaskTleName())
                .append(" where STATUS = ").append(SEP).append(TaskStatus.DELAY).append(SEP)
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
                result.add(new MysqlTask(new TaskInfo(id, db, table, partitions, parName, filePath, op)));
            }
        } catch (Exception e) {
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
