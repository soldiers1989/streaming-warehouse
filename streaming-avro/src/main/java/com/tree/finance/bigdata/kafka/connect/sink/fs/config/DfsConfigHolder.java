package com.tree.finance.bigdata.kafka.connect.sink.fs.config;

import com.tree.finance.bigdata.kafka.connect.sink.fs.utils.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/20 10:47
 */
public class DfsConfigHolder {

    private static volatile boolean initialized = false;

    private static Configuration conf = new Configuration();

    private static final Logger LOG = LoggerFactory.getLogger(DfsConfigHolder.class);

    public static Configuration getConf() {
        return conf;
    }

}
