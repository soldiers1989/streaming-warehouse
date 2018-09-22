package com.tree.finance.bigdata.hive.streaming.tools.config;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/21 14:06
 */
public interface Constants {
    String HIVE_CREATE_TBL_TEMPLATE_FILE = "/hive_create_table.template";
    String MYSQL_DB_CONF_FILE = "/mysql.database.properties";
    String MYSQL_DB_USER = "source.db.user";
    String MYSQL_DB_PASSWORD = "source.db.password";
    String KEY_HBASE_RECORDID_TBL_SUFFIX = "_id";
}
