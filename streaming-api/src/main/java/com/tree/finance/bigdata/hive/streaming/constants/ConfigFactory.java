package com.tree.finance.bigdata.hive.streaming.constants;

import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
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

    public static String getHbaseRecordIdTbl() {
        return properties.getProperty(Constants.KEY_HBASE_RECORDID_TBL);
    }

    public static String getHbaseZnodeParent() {
        return properties.contains(Constants.KEY_HBASE_ZNODE_PARENT) ?
        properties.getProperty(Constants.KEY_HBASE_ZNODE_PARENT) : "/hbase";
    }
}
