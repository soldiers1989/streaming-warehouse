package com.tree.finance.bigdata.hive.streaming.constants;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/4 09:58
 */
public interface Constants {

    String GLOBAL_UPDATE_COLUMN = "record.global.update.column";

    String COLUMN_UPDATE_IDENTIFIER = "record.column.update.identifier";
    String COLUMN_TIME_IDENTIFIER = "record.column.time.identifier";
    String COLUMN_CREATE_IDENTIFIER = "record.column.create.identifier";
    String MUTATION_COLUMN_CONF = "/mutation.properties";

    String KEY_HBASE_INSERT_BATCH_SIZE = "hbase.insert.batch.size";
    String KEY_HBASE_TABLE_NAME = "hbase.table.name";
    String KEY_HBASE_RECORD_ID_COL_IDENTIFIER = "hbase.record.id.column.identifier";
    String KEY_HBASE_UPDATE_TIEM_COL_IDENTIFIER = "hbase.update.time.column.identifier";
    String KEY_HBASE_DEFAULT_COLUMN_FAMILY = "hbase.default.column.family";
    String KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    String KEY_HBASE_RECORDID_TBL_SUFFIX = "_id";
    String KEY_HBASE_ZNODE_PARENT = "hbase.znode.parent";

    String KEY_HBASE_CHECK_UPDATE_TIME_COL = "key.hbase.fix.update.time.col";
    String KEY_HBASE_STREAM_UPDATE_TIME_COL = "key.hbase.stream.update.time.col";
    String KEY_HBASE_SYS_CONF_TBL = "key.hbase.sys.conf.tbl";


}
