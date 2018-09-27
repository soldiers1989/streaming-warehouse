package com.tree.finance.bigdata.hive.streaming.config.imutable;

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
    @Config("rabbit.mq.qos")
    @Default("10")
    public abstract int getRabbitQos();

    //app resource config
    @Config("file.queue.size")
    @Default("100")
    public abstract Integer getFileQueueSize();
    @Config("processor.insert.cores")
    @Default("10")
    public abstract Integer getInsertProcessorCores();
    @Config("processor.update.cores")
    @Default("5")
    public abstract Integer getUpdateProcessorCores();
    @Config("processor.delay.cores")
    @Default("0")
    public abstract int getDelayProcessorCores();

    //hive config
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
    @Config("delete.intermediate.file.on.success")
    @Default("false")
    public abstract boolean deleteAvroOnSuccess();
    @Config("task.greedy.process.batch.limit")
    @Default("10")
    public abstract int getGreedyProcessBatchLimit();
    @Config("database.task.info.on.success.strategy")
    @Default("update")
    public abstract String getDBTaskInfoStrategyOnSuccess();
    @Config("task.delay.schedule.check.interval.min")
    @Default("5")
    public abstract int getDelayScheduleMin();
    @Config("task.delay.max.retries")
    @Default("5")
    public abstract int getDelayTaskMaxRetries();
    @Config("task.resources")
    @Default("mq")
    public abstract String[] getTaskResources();


    //task database config
    @Config("task.db.url")
    public abstract String getTaskDbUrl();
    @Config("task.db.user")
    public abstract String getTaskDbUser();
    @Config("task.db.password")
    public abstract String getTaskDbPassword();
    @Config("task.tbl.name")
    @Default("task_info")
    public abstract String getTaskTleName();

    @Config("intermediate.avro.backup.path")
    @Default("/data/kafka-connect/done/")
    public abstract String getIntermediateBackUpPath();
}
