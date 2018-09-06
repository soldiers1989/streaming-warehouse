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
        try{
            Properties prop = new Properties();
            prop.load(RabbitMqUtils.class.getResourceAsStream(MUTATION_COLUMN_CONF));
            return prop;
        }catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    public static Properties getConfig() {
        return properties;
    }
}
