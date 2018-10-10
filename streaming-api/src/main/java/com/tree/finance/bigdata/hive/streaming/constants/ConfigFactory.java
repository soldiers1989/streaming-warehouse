package com.tree.finance.bigdata.hive.streaming.constants;

import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.MUTATION_COLUMN_CONF;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 10:19
 */
public class ConfigFactory {

    static Properties properties = loadProperties();

    private static Logger LOG = LoggerFactory.getLogger(ConfigFactory.class);

    private static Configuration hbaseConf = initHbaseConfig();

    private static Properties loadProperties() {
        try {
            Properties prop = new Properties();
            prop.load(RabbitMqUtils.class.getResourceAsStream(MUTATION_COLUMN_CONF));
            return prop;
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    private static Configuration initHbaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ConfigFactory.getHbaseZookeeperQuorum());
        conf.set("zookeeper.znode.parent", ConfigFactory.getHbaseZnodeParent());
        return conf;
    }

    private static String getHbaseZnodeParent() {
        return properties.contains(Constants.KEY_HBASE_ZNODE_PARENT) ?
                properties.getProperty(Constants.KEY_HBASE_ZNODE_PARENT) : "/recId";
    }

    public static Configuration getHbaseConf() {
        return hbaseConf;
    }

    public static Properties getConfig() {
        return properties;
    }


    public static String getHbaseRecordIdColumnIdentifier() {
        return properties.getProperty(Constants.KEY_HBASE_RECORD_ID_COL_IDENTIFIER);
    }

    public static String getHbaseUpdateTimeColumnIdentifier() {
        return properties.getProperty(Constants.KEY_HBASE_UPDATE_TIEM_COL_IDENTIFIER);
    }

    public static String getHbaseColumnFamily() {
        return properties.contains(Constants.KEY_HBASE_DEFAULT_COLUMN_FAMILY) ?
                properties.getProperty(Constants.KEY_HBASE_DEFAULT_COLUMN_FAMILY) : "f";
    }

    public static String getHbaseZookeeperQuorum() {
        return properties.getProperty(Constants.KEY_HBASE_ZOOKEEPER_QUORUM);
    }

    public static String getSysConfHbaseTbl() {
        return properties.contains(Constants.KEY_HBASE_SYS_CONF_TBL) ?
        properties.getProperty(Constants.KEY_HBASE_SYS_CONF_TBL) : "streaming_warehouse_system_conf";
    }
    public static String getStreamUpdateTimeQualifier() {
        return properties.contains(Constants.KEY_HBASE_STREAM_UPDATE_TIME_COL) ?
                properties.getProperty(Constants.KEY_HBASE_STREAM_UPDATE_TIME_COL) : "stream_update_time";
    }
    public static String getCheckUpdateTimeQualifier() {
        return properties.contains(Constants.KEY_HBASE_CHECK_UPDATE_TIME_COL) ?
                properties.getProperty(Constants.KEY_HBASE_CHECK_UPDATE_TIME_COL) : "check_update_time";
    }

}
