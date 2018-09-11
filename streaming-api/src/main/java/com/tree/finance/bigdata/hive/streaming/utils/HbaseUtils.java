package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_INSERT_BATCH_SIZE;
import static com.tree.finance.bigdata.hive.streaming.constants.Constants.KEY_HBASE_TABLE_NAME;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 11:35
 */
public class HbaseUtils {

    static Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);
    static Connection connection;
    private Integer batchSize;

    private List<Put> buffer;
    private Table htable;

    private HbaseUtils(String tableName, Configuration config) throws IOException {
        this.buffer = new ArrayList<>();
        this.batchSize = config.getInt(KEY_HBASE_INSERT_BATCH_SIZE, 500);
        if (connection == null) {
            synchronized (HbaseUtils.class) {
                if (connection == null) {
                    config.setInt("zookeeper.session.timeout", 3600000);
                    connection = ConnectionFactory.createConnection(config);
                }
            }
        }
        this.htable = connection.getTable(TableName.valueOf(tableName));
    }

    public static HbaseUtils getTableInstance(String tableName, Configuration conf) throws IOException {
        if (StringUtils.isEmpty(tableName)) {
            throw new RuntimeException(KEY_HBASE_TABLE_NAME + " is not set");
        }
        return new HbaseUtils(tableName, conf);
    }

    public String getString(String rowKey, byte[] family, byte[] col) {
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

    public Long getLong(String rowKey, byte[] family, byte[] col) {
        try {
            Get getVal = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(getVal);
            byte[] value = result.getValue(family, col);
            if (value == null) {
                return null;
            } else {
                return Bytes.toLong(value);
            }
        } catch (Exception e) {
            LOG.error("failed to get rowId from HBase rowKey: {}\n{}" + rowKey, e);
            throw new RuntimeException(e);
        }
    }

    public Long[] getStringsAsLongs(String rowKey, byte[] family, byte[]... cols) {
        try {
            Get getVal = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(getVal);
            Long[] values = new Long[cols.length];
            for (int i = 0; i < cols.length; i++) {
                byte[] value = result.getValue(family, cols[i]);
                if (value == null) {
                    continue;
                } else {
                    values[i] = Long.valueOf(Bytes.toString(value));
                }
            }
            return values;
        } catch (Exception e) {
            LOG.error("failed to get rowId from HBase rowKey: {}\n{}" + rowKey, e);
            throw new RuntimeException(e);
        }
    }


    public Object[] getAsBytes(String rowKey, byte[] family, byte[]... cols) {
        try {
            Get getVal = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(getVal);
            Object[] values = new Object[cols.length];
            for (int i = 0; i < cols.length; i++) {
                byte[] value = result.getValue(family, cols[i]);
                values[i] = value;
            }
            return values;
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
            htable.put(buffer);
            buffer.clear();
        }
        buffer.add(put);
    }

    public void put(Put put) throws IOException {
        htable.put(put);
    }

    public void close() throws IOException {
        if (!buffer.isEmpty()) {
            htable.put(buffer);
        }
        htable.close();
    }

}
