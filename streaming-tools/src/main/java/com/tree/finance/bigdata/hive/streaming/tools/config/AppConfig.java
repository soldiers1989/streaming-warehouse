package com.tree.finance.bigdata.hive.streaming.tools.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/6 16:22
 */
public abstract class AppConfig {

    //hive config
    @Config("metastore.uris")
    public abstract String getMetastoreUris();
    //used to create hive tables
    @Config("hiveserver2.jdbc.url")
    public abstract String getHiveServer2Url();

}
