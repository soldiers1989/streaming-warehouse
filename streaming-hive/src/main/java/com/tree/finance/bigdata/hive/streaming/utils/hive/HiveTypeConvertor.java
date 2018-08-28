package com.tree.finance.bigdata.hive.streaming.utils.hive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 22:41
 */
public class HiveTypeConvertor {

    private static final String HIVE_TYPE_INT = "INT";
    private static final String HIVE_TYPE_STRING = "STRING";
    private static final String HIVE_TYPE_BIGINT = "BIGINT";
    private static final String HIVE_TYPE_TIMESTAMP = "TIMESTAMP";

    public static String toHiveType(String rawSqlType) {

        String sqlType = rawSqlType.replaceAll(" ", "");

        //extract mysql data type. eg: tinyint(1) -> tinyint, int(11) unsigned -> int
        String regex = "(\\w+)(\\([0-9]+\\))";
        Pattern pattern = Pattern.compile(regex);
        Matcher m = pattern.matcher(sqlType);
        if (m.find()) {
            sqlType = m.group(1);
        }

        //double(2,3) -> double
        String regex2 = "(\\w+)(\\([0-9]+,[0-9]+\\))";
        Pattern pattern2 = Pattern.compile(regex2);
        Matcher m2 = pattern2.matcher(sqlType);
        if (m2.find()) {
            sqlType = m2.group(1);
        }

        //convert to int
        if(sqlType.equalsIgnoreCase(MysqlType.MEDIUMINT.name())) {
            return HIVE_TYPE_INT;
        }
        if(sqlType.equalsIgnoreCase(MysqlType.TINYINT.name())) {
            return HIVE_TYPE_INT;
        }
        if(sqlType.equalsIgnoreCase(MysqlType.SMALLINT.name())) {
            return HIVE_TYPE_INT;
        }
        if (sqlType.equalsIgnoreCase(MysqlType.INT.name())) {
            if (rawSqlType.toLowerCase().contains("unsigned")) {
                return HIVE_TYPE_BIGINT;
            }
        }
        //convert to string
        if (sqlType.equalsIgnoreCase(MysqlType.VARCHAR.name())) {
            return HIVE_TYPE_STRING;
        }
        if (sqlType.startsWith(MysqlType.ENUM.name().toLowerCase())
                || sqlType.startsWith(MysqlType.ENUM.name().toUpperCase())) {
            return HIVE_TYPE_STRING;
        }
        if (sqlType.equalsIgnoreCase(MysqlType.CHAR.name())) {
            return HIVE_TYPE_STRING;
        }
        if (sqlType.equalsIgnoreCase(MysqlType.TEXT.name())) {
            return HIVE_TYPE_STRING;
        }
        if (sqlType.equalsIgnoreCase(MysqlType.LONGTEXT.name())) {
            return HIVE_TYPE_STRING;
        }
        if (sqlType.equalsIgnoreCase(MysqlType.MEDIUMTEXT.name())) {
            return HIVE_TYPE_STRING;
        }
        //BigDecimal
        if (sqlType.equalsIgnoreCase(MysqlType.DECIMAL.name())) {
            return rawSqlType.toLowerCase().replaceAll("unsigned", "");
        }
        //datetime
        if (sqlType.equalsIgnoreCase(MysqlType.DATETIME.name())) {
            return HIVE_TYPE_TIMESTAMP;
        }
        //not support
        if (sqlType.equalsIgnoreCase(MysqlType.BIT.name())) {
            throw new RuntimeException("unsupported type BIT");
        }
        return sqlType;
    }


    enum MysqlType {
        MEDIUMINT,
        BIT,
        CHAR,
        VARCHAR,
        ENUM,
        TEXT,
        TINYINT,
        SMALLINT,
        INT,
        LONGTEXT,
        DECIMAL,
        DATETIME,
        MEDIUMTEXT
    }
}
