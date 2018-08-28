package com.tree.finance.bigdata.schema;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/16 17:46
 */
public enum LogicalType {

    ZonedTimestamp("io.debezium.time.ZonedTimestamp"),

    Date("date"),

    Decimal("decimal"),

    TimeStampMillis("timestamp-millis");

    private String value;

    LogicalType(String value){
        this.value = value;
    }
    public String value(){
        return value;
    }
}
