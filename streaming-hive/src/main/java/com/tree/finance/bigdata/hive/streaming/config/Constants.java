package com.tree.finance.bigdata.hive.streaming.config;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 11:14
 */
public interface Constants {
    String HIVE_CREATE_TBL_TEMPLATE_FILE = "/hive_create_table.template";
    String FILE_PROCESSED_SUFIX =  ".processed";
    String MYSQL_DB_CONF_FILE = "/mysql.database.properties";
    String MYSQL_DB_USER = "source.db.user";
    String MYSQL_DB_PASSWORD = "source.db.password";
    String SPACE = " ";
    String SQL_VALUE_QUOTE = "'";
}
