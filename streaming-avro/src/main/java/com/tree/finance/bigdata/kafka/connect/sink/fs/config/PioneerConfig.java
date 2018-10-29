package com.tree.finance.bigdata.kafka.connect.sink.fs.config;

import com.tree.finance.bigdata.utils.common.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

import static com.tree.finance.bigdata.kafka.connect.sink.fs.config.ConfKeys.*;

public class PioneerConfig {

    static final String CONF_FILE = "pioneer.conf.file";


    private static Logger LOG = LoggerFactory.getLogger(PioneerConfig.class);

    private static Map conf = init();

    private static Map databasesMap = (Map) conf.get(DBS);

    static String DFS_FILE_SEPARATOR = "/";
    static int DEFAULT_DB_CORES = 1;
    static int DEFAULT_SHUTDOWN_PORT = 8888;
    static String CONSUMER_GROUP_SUFFIX = "_avro";

    public static Map init() {
        try {
            Yaml yaml = new Yaml();
            String confFile = System.getProperty(CONF_FILE);
            if (StringUtils.isEmpty(confFile)) {
                throw new RuntimeException(CONF_FILE + " not set");
//                return yaml.load(PioneerConfig.class.getResourceAsStream("/pioneer.yaml"));
            } else {
                return yaml.load(new FileInputStream(new File(confFile)));
            }
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    public static Map getKafkaConf() {
        Map kafkaConf = new HashMap();
        Map global = (Map) conf.get(KAFKA_CONF_GLOBAL);
        kafkaConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaConf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaConf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaConf.putAll(global);
        return kafkaConf;
    }

    public static String getConnectSchemaUrl() {
        return conf.get(CONNECT_SCHEMA_URL_KEY).toString();
    }


    public static void main(String[] args) {
        System.out.println(Arrays.toString(getTopics("clientrelationship")));
    }

    public static String getConsumerGroupId(String db) {
        String clusterId = conf.get(CLUSTER_NAME).toString();
        Map dbConf = ((Map) databasesMap.get(db));
        if (dbConf.containsKey(DB_KAFKA_CLIENT)) {
            return dbConf.get(DB_KAFKA_CLIENT).toString();
        } else {
            return clusterId + "_" + db + CONSUMER_GROUP_SUFFIX;
        }
    }

    public static String[] getTopics(String db) {
        Map dbConf = ((Map) databasesMap.get(db));
        ArrayList<String> toplicList = (ArrayList<String>) dbConf.get(DB_TOIPCS);
        return toplicList.toArray(new String[toplicList.size()]);
    }

    public static List<String> getSubscribeDbs() {
        return  (ArrayList<String>) conf.get(SUBSCRIBE_DBS);
    }

    public static String getDefaultParClos() {
        return conf.get(PARTITION_CONFIG).toString();
    }

    public static String getHiveDestinationDbName(String sourceDb) {
        Map dbConf = (Map) databasesMap.get(sourceDb);
        if (dbConf.containsKey(DB_HIVE_TARGET)) {
            return dbConf.get(DB_HIVE_TARGET).toString();
        } else {
            return sourceDb;
        }
    }

    public static int getDbCores(String db) {
        Map dbConf = (Map) databasesMap.get(db);
        if (dbConf.containsKey(DB_CORES)) {
            return Integer.valueOf(dbConf.get(DB_CORES).toString());
        } else {
            return DEFAULT_DB_CORES;
        }
    }

    public static int getTotalCores() {
        int totalCores = 0;
        for (String db : (Set<String>) databasesMap.keySet()) {
            totalCores += getDbCores(db);
        }
        return totalCores;
    }

    public static String getRabbitHost() {
        return conf.get(MQ_HOST).toString();
    }

    public static int getRabbitPort() {
        return Integer.valueOf(conf.get(MQ_PORT).toString());
    }

    public static String getRabbitMqTaskQueue() {
        return conf.get(TASK_QUEUE_NAME).toString();
    }

    public static String getTaskDbUrl() {
        return conf.get(TASK_JDBC_URL).toString();
    }

    public static String getTaskDbUser() {
        return conf.get(TASK_JDBC_USER).toString();
    }

    public static String getTaskDbPassword() {
        return conf.get(TASK_JDBC_PWD).toString();
    }

    public static String getTaskTable() {
        return conf.get(TASK_TBL_NAME).toString();
    }

    public static String getWriterBasePath() {
        return conf.get(WRITER_PATH).toString();
    }

    public static long getWriterMaxMsg() {
        return Long.valueOf(conf.get(WRITER_MAX_MSGS).toString());
    }

    public static int getWriterTTLMin() {
        return Integer.valueOf(conf.get(WRITER_TTL).toString());
    }

    public static Map getDBConfs() {
        return databasesMap;
    }

    public static int getShutDownPort() {
        if (conf.containsKey(SHUTDOWN_PORT)) {
            return Integer.valueOf(conf.get(SHUTDOWN_PORT).toString());
        } else {
            return DEFAULT_SHUTDOWN_PORT;
        }
    }
}
