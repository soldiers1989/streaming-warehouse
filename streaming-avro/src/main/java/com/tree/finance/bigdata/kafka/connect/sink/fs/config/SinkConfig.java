package com.tree.finance.bigdata.kafka.connect.sink.fs.config;

import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

import static com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig.Default.*;
import static com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig.Validator.NON_NEGATIVE_INT_VALIDATOR;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/6/29 10:34
 */
public class SinkConfig extends AbstractConfig {

    private Map<String, String> dynamicConf;

    public SinkConfig(Map<String, String> map) {
        super(CONFIG_DEF, map);
        this.dynamicConf = map;
    }


    public static final ConfigDef CONFIG_DEF = new ConfigDef()

            //database config
            .define(KEY.HIVE_DATABASE_NAME, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "destination database name in hive")

            //Writer options
            .define(KEY.WRITER_MAX_INSERT_MSG, ConfigDef.Type.LONG, 3000000, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, "")
            .define(KEY.WRITER_MAX_UPDATE_MSG, ConfigDef.Type.LONG, 500000, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, "")

            .define(KEY.WRITER_TTL_MIN, ConfigDef.Type.INT, 5, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, "")
            .define(KEY.WRITER_BASE_PATH, ConfigDef.Type.STRING, Default.WRITER_BASE_PATH_DEFAULT,
                    ConfigDef.Importance.HIGH, "")
            .define(KEY.SINK_WRITER_CLASS, ConfigDef.Type.STRING, SINK_WRITER_CLASS_DEFAULT,
                    ConfigDef.Importance.HIGH, "")

            //HBase table name used to store source table's partition column config
            .define(KEY.TABLE_PARTITION_CONFIG_GLOBAL, ConfigDef.Type.STRING, TABLE_PARTITION_CONFIG_GLOBAL_VALUE,
                    ConfigDef.Importance.MEDIUM, "default table partition name")

            //Rabbit Mq
            .define(KEY.RABBIT_MQ_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "")
            .define(KEY.RABBIT_MQ_PORT, ConfigDef.Type.INT, ConfigDef.Importance.MEDIUM, "")
            .define(KEY.RABBIT_MQ_TASK_QUEUE, ConfigDef.Type.STRING, RABBIT_MQ_TASK_QUEUE_DEFAULT,
                    ConfigDef.Importance.HIGH, "")

            //HADOOP
            .define(KEY.HADOOP_CONF_PATH, ConfigDef.Type.STRING, HADOOP_CONF_PATH_DEFAULT,
                    ConfigDef.Importance.HIGH, "")

            //Task DB
            .define(KEY.TASK_DB_JDBC_URL, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "task db jdbc url")
            .define(KEY.TASK_DB_USER, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "task db user")
            .define(KEY.TASK_DB_PASSWORD, ConfigDef.Type.STRING, null, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "task db password")
            .define(KEY.TASK_TABLE_NAME, ConfigDef.Type.STRING, "task_info", new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.MEDIUM, "task info table name");


    public String getWriterClass() {
        return AvroWriter.class.getName();
    }

    public long getWriterMaxInsertMsg() {
        return getLong(KEY.WRITER_MAX_INSERT_MSG);
    }

    public long getWriterMaxUpdateMsg() {
        return getLong(KEY.WRITER_MAX_UPDATE_MSG);
    }

    public String getTaskDbUrl() {
        return getString(KEY.TASK_DB_JDBC_URL);
    }

    public String getTaskDbUser() {
        return getString(KEY.TASK_DB_USER);
    }

    public String getTaskDbPassword() {
        return getString(KEY.TASK_DB_PASSWORD);
    }

    public String getTaskTable() {
        return getString(KEY.TASK_TABLE_NAME);
    }

    public interface KEY {
        //mysql表中的分区字段
        String TABLE_PARTITION_CONFIG_GLOBAL = "table.partition.config.global";
        //单个文件句柄最大写入消息数
        String WRITER_MAX_INSERT_MSG = "sink.writer.max.write.insert.msg.per.file";
        String WRITER_MAX_UPDATE_MSG = "sink.writer.max.write.update.msg.per.file";

        //单个文件句柄最大打开时间
        String WRITER_TTL_MIN = "sink.writer.ttl.min";
        //写文件根目录
        String WRITER_BASE_PATH = "sink.writer.base.path";
        //TaskId
        String SINK_TASK_ID = "sink.task.id";

        //RabbitMq
        String RABBIT_MQ_HOST = "rabbit.mq.host";
        String RABBIT_MQ_PORT = "rabbit.mq.port";
        String RABBIT_MQ_TASK_QUEUE = "rabbit.mq.task.queue";

        //Hadoop
        String HADOOP_CONF_PATH = "hadoop.conf.dir";
        String SINK_WRITER_CLASS = "sink.writer.class";

        //topic to database mapping
        String HIVE_DATABASE_NAME = "hive.destination.database.name";

        //database used to store task info
        String TASK_DB_JDBC_URL = "task.db.jdbc.url";
        String TASK_DB_USER = "task.db.user";
        String TASK_DB_PASSWORD = "task.db.password";
        String TASK_TABLE_NAME = "task.table.name";
    }

    interface Validator {
        ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);
        ConfigDef.Validator NO_EMPTY = (s, o) -> {
            System.out.println(s + " : " + o);
            if (o == null || o.toString().isEmpty()) throw new ConfigException(s + " not configured");
        };
    }

    public interface Default {
        String DFS_FILE_SEPARATOR = "/";
        String WRITER_BASE_PATH_DEFAULT = "/data/kafka-connect/sink/";
        String TABLE_PARTITION_CONFIG_GLOBAL_VALUE = "create,creat|date,time,createat,creatat,createdat";
        String RABBIT_MQ_TASK_QUEUE_DEFAULT = "streaming_warehouse_file_queue";
        String HADOOP_CONF_PATH_DEFAULT = "/etc/hadoop/conf";
        String SINK_WRITER_CLASS_DEFAULT = "com.tree.finance.bigdata.kafka.connect.sink.fs.writer.AvroWriter";
    }


    public String getWriterBasePath() {
        return getString(KEY.WRITER_BASE_PATH);
    }

    public int getWriterTTLMin() {
        return getInt(KEY.WRITER_TTL_MIN);
    }

    public int getTaskId() {
        String id = dynamicConf.get(KEY.SINK_TASK_ID);
        return Integer.valueOf(id);
    }

    public String getRabbitMqHost() {
        return getString(KEY.RABBIT_MQ_HOST);
    }

    public int getRabbitMqPort() {
        return getInt(KEY.RABBIT_MQ_PORT);
    }

    public String getRabbitMqTaskQueue() {
        return getString(KEY.RABBIT_MQ_TASK_QUEUE);
    }

    public String getDefaultParClos() {
        return getString(KEY.TABLE_PARTITION_CONFIG_GLOBAL);
    }

    public String getHadoopConfDir() {
        return getString(KEY.HADOOP_CONF_PATH);
    }

    public String getHiveDestinationDbName() {
        return getString(KEY.HIVE_DATABASE_NAME);
    }

}
