package com.tree.finance.bigdata.hive.streaming.config;

import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/6 16:25
 */
public class ConfigurationBuilder {

    public static AppConfig build(Properties properties){
        return new ConfigurationObjectFactory(properties).build(AppConfig.class);
    }

}
