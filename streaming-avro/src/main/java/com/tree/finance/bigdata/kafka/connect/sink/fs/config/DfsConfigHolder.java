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

    private static Configuration conf = null;

    private static final Logger LOG = LoggerFactory.getLogger(DfsConfigHolder.class);

    public static synchronized void init(SinkConfig sinkConfig) {
        if (initialized) {
            return;
        } else {
            conf = new Configuration();
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            String confDir = sinkConfig.getHadoopConfDir();
            if (StringUtils.isEmpty(confDir)) {
                LOG.info("no hadoop config set");
                initialized = true;
                return;
            } else {
                LOG.info("configured hadoop conf dir: {}", confDir);
                conf.addResource(new Path(confDir + "/core-site.xml"));
                conf.addResource(new Path(confDir + "/hdfs-site.xml"));
                initialized = true;
            }
        }
    }


    public static Configuration getConf() {
        if (initialized) {
            return conf;
        } else {
            throw new RuntimeException("hadoop config not set yet");
        }
    }

}
