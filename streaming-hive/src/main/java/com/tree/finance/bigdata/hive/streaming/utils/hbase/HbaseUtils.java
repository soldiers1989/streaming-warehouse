package com.tree.finance.bigdata.hive.streaming.utils.hbase;

import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 11:35
 */
public class HbaseUtils {

    static Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);
    static Configuration config = HBaseConfiguration.create();
    static Connection connection;
    static final String zkStr = ConfigHolder.getConfig().getHbaseZkQuorum();
    static final String zkRoot = ConfigHolder.getConfig().getHbaseZkRoot();
    static Integer batchSize = ConfigHolder.getConfig().getHbaseBatchSize();

    private List<Put> buffer;
    private Table htable;

    static {
        try {
            config.set("hbase.zookeeper.quorum", zkStr);
            config.set("zookeeper.znode.parent", zkRoot);
            connection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private HbaseUtils(String tableName) throws IOException {
        this.buffer = new ArrayList<>();
        this.htable = connection.getTable(TableName.valueOf(tableName));
    }

    public static HbaseUtils getTableInstance(String table) throws IOException {
        return new HbaseUtils(table);
    }

    public String getValue(String rowKey, byte[] family, byte[] col) {
        try {
            Get getVal = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(getVal);
            byte[] value = result.getValue(family, col);
            if (value == null) {
                return null;
            } else {
                return new String(value);
            }
        } catch (Exception e) {
            LOG.error("failed to get rowId from HBase rowKey: {}\n{}" + rowKey, e);
            throw new RuntimeException(e);
        }
    }

    public static void createTale(String tableName, String... clomnFamily) throws Exception {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor family = new HColumnDescriptor(("f").getBytes());
        table.addFamily(family);
        Admin admin = connection.getAdmin();
        admin.createTable(table);
        admin.close();
    }

    public void insertAsync(Put put) throws IOException {
        if (buffer.size() >= batchSize) {
            htable.put(put);
            buffer.clear();
        }
        buffer.add(put);
    }

    public void close() throws IOException {
        if (!buffer.isEmpty()) {
            htable.put(buffer);
        }
        htable.close();
    }

}
