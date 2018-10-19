package com.tree.finance.bigdata.kafka.connect.sink.fs.config;

public interface ConfKeys {

    String CLUSTER_NAME = "cluster.name";
    String PARTITION_CONFIG = "table.partition.config.global";
    String SHUTDOWN_PORT = "server.shutdown.listener.port";

    String MQ_HOST = "rabbit.mq.host";
    String MQ_PORT = "rabbit.mq.port";

    String WRITER_TTL = "sink.writer.ttl.min";
    String WRITER_MAX_MSGS = "writer.max.msg.per.file";
    String WRITER_PATH = "writer.base.path";

    String TASK_JDBC_URL = "task.db.jdbc.url";
    String TASK_JDBC_USER = "task.db.user";
    String TASK_JDBC_PWD = "task.db.password";
    String TASK_QUEUE_NAME = "rabbit.mq.task.queue";
    String TASK_TBL_NAME = "task.storage.table.name";

    String DBS = "databases";
    String DB_NAME = "db.name";
    String DB_HIVE_TARGET = "hive.destination.database.name";
    String DB_CORES = "cores";
    String DB_TOIPCS = "topics";
    String DB_KAFKA_CLIENT = "kafka.client.name";

    String KAFKA_CONF_GLOBAL = "kafka.global";
    String CONNECT_SCHEMA_URL_KEY = "connect.schema.url";
}
