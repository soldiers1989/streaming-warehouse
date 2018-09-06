package com.tree.finance.bigdata.hive.streaming.constants;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 09:58
 */
public interface Constants {
    String COLUMN_UPDATE_IDENTIFIER = "record.column.update.identifier";
    String COLUMN_TIME_IDENTIFIER = "record.column.time.identifier";
    String COLUMN_CREATE_IDENTIFIER = "record.column.create.identifier";
    String MUTATION_COLUMN_CONF = "/mutation.properties";

    String KEY_HBASE_INSERT_BATCH_SIZE = "hbase.insert.batch.size";
    String KEY_HBASE_TABLE_NAME = "hbase.table.name";
}
