package com.tree.finance.bigdata.hive.streaming.config.imutable;

import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/11 10:13
 */
public class ConfigHolder {

    private static final String CONF_KEY = "app.config.file";

    private static Logger LOG = LoggerFactory.getLogger(ConfigHolder.class);

    private static AppConfig config = initAppConfig();

    private static Configuration hbaseConf = initHbaseConfig();

    private static Configuration initHbaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ConfigHolder.getConfig().getHbaseZkQuorum());
        conf.set("zookeeper.znode.parent", ConfigHolder.getConfig().getHbaseZkRoot());
        conf.set("hbase.table.name", ConfigHolder.getConfig().getRowIdToRecIdHbaseTbl());
        return conf;
    }

    private static AppConfig initAppConfig() {
        try {
            Properties properties = new Properties();
            String confFile = System.getProperty(CONF_KEY);
            if (StringUtils.isEmpty(confFile)) {
                properties.load(ConfigHolder.class.getResourceAsStream("/program.properties"));
            } else {
                properties.load(new FileInputStream(new File(confFile)));
            }
            return ConfigurationBuilder.build(properties);
        } catch (Exception e) {
            LOG.error("error loading app config", e);
            throw new RuntimeException(e);
        }
    }

    public static Configuration getHbaseConf() {
        return hbaseConf;
    }

    public static AppConfig getConfig() {
        return config;
    }
}
