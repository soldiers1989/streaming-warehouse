package com.tree.finance.bigdata.hive.streaming.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/6 16:22
 */
public abstract class AppConfig {


    //rabbit config
    @Config("rabbit.mq.host")
    public abstract String getRabbitHost();
    @Config("rabbit.mq.port")
    public abstract Integer getRabbitPort();
    @Config("rabbit.mq.task.queue.name")
    @Default("streaming_warehouse_file_queue")
    public abstract String getRabbitQueueName();
    @Config("rabbit.mq.delay.task.queue.name")
    @Default("streaming_warehouse_delay_file_queue")
    public abstract String getRabbitDelayQueueName();
    @Config("rabbit.mq.error.task.queue.name")
    @Default("streaming_warehouse_error_file_queue")
    public abstract String getRabbitErrorQueueName();
    @Config("rabbit.mq.qos")
    @Default("500")
    public abstract int getRabbitQos();

    //app resource config
    @Config("file.queue.size")
    @Default("100")
    public abstract Integer getFileQueueSize();
    @Config("processor.insert.cores")
    @Default("10")
    public abstract Integer getInsertProcessorCores();
    @Config("processor.update.cores")
    @Default("2")
    public abstract Integer getUpdateProcessorCores();

    //hbase config
    @Config("hbase.rowId.recId.tbl")
    @Default("streaming_warehouse_rowId2recId_tbl")
    public abstract String getRowIdToRecIdHbaseTbl();
    @Config("hbase.default.col.family")
    @Default("f")
    public abstract String getDefaultColFamily();
    @Config("hbase.recordId.col.qualifier")
    @Default("recordId")
    public abstract String getRecordIdQualifier();
    @Config("hbase.update.time.col.qualifier")
    @Default("update_time")
    public abstract String getUpdateTimeQualifier();
    @Config("hbase.batch.size")
    @Default("100")
    public abstract Integer getHbaseBatchSize();
    @Config("hbase.zookeeper.quorum")
    public abstract String getHbaseZkQuorum();
    @Config("zookeeper.znode.parent")
    @Default("/hbase")
    public abstract String getHbaseZkRoot();

    //task retry config
    @Config("task.retries.on.data.delayed")
    @Default("2")
    public abstract Integer getTaskRetriesOnDataDelay();
    @Config("task.retry.interval.sec.on.data.delay")
    @Default("120")
    public abstract Integer getTaskRetryIntervalSecOnDataDelay();
    @Config("task.retries.on.error")
    @Default("3")
    public abstract Integer getTaskRetriesOnError();

    //hive config
    @Config("hive.table.cluster.columns")
    @Default("createdate,createtime,creatdate,creattime,createddatetime,createdtime,create_date")
    public abstract String getDefaultClusterCols();
    @Config("metastore.uris")
    public abstract String getMetastoreUris();
    //used to create hive tables
    @Config("hiveserver2.jdbc.url")
    public abstract String getHiveServer2Url();

    //prometheus
    @Config("prometheus.server.port")
    @Default("57891")
    public abstract Integer getPrometheusServerPort();

    //process strategy config
    @Config("batch.trigger.queue.size")
    @Default("50")
    public abstract int getBatchTriggerQueueSize();
    @Config("delete.intermediate.file.on.success")
    @Default("true")
    public abstract boolean deleteAvroOnSuccess();

    //time identifier
    @Config("record.column.time.identifier")
    @Default("time,date,createat,createdat,updateat,updatedat")
    public abstract String timeColIdentifier();
    //update identifier
    @Config("record.column.update.identifier")
    @Default("update,updat")
    public abstract String updateColIdentifier();
    //create identifier
    @Config("record.column.create.identifier")
    @Default("create,creat")
    public abstract String createColIdentifier();

}
