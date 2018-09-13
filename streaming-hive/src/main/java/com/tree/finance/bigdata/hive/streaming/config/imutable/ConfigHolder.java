package com.tree.finance.bigdata.hive.streaming.config.imutable;

import com.tree.finance.bigdata.utils.common.StringUtils;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
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

    private static HiveConf hiveConf = initHiveConf();

    private static ConnectionFactory factory = initFactory();

    private static ConnectionFactory initFactory() {
        return new ConnectionFactory.Builder().jdbcUrl(config.getTaskDbUrl())
                .user(config.getTaskDbUser()).password(config.getTaskDbPassword())
                .acquireIncrement(1).initialPoolSize(5).maxPollSize(10).build();
    }

    private static HiveConf initHiveConf() {
        HiveConf conf = new HiveConf();
        conf.setInt(MutatorClient.TRANSACTIONAL_LOCK_RETIES_KEY, 1);
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

    public static HiveConf getHiveConf() {
        return hiveConf;
    }

    public static AppConfig getConfig() {
        return config;
    }

    public static ConnectionFactory getDbFactory() {
        return factory;
    }
}
