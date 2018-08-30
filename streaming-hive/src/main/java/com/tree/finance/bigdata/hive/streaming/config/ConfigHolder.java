package com.tree.finance.bigdata.hive.streaming.config;

import com.tree.finance.bigdata.hive.streaming.utils.common.StringUtils;
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

    public static AppConfig getConfig() {
        return config;
    }
}
