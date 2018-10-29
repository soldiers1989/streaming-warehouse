package com.tree.finance.bigdata.hive.streaming.utils.metric;

import com.tree.finance.bigdata.hive.streaming.utils.MutateResult;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.utils.network.NetworkUtils;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/26 19:09
 */
public class MetricReporter {

    private static Counter consumedMq = Counter.build().name("streaming_hive_mq_consumed")
            .help("rabbit mq message consumed")
            .labelNames("ip", "type")
            .register();

    private static Counter processedFiles = Counter.build().name("streaming_hive_processed_files")
            .help("combined opration files processed")
            .labelNames("ip")
            .register();

    private static Counter processedInsertFiles = Counter.build().name("streaming_hive_insert_processed_files")
            .help("insert files processed")
            .labelNames("ip")
            .register();

    private static Counter processedUpdateFiles = Counter.build().name("streaming_hive_update_processed_files")
            .help("update files processed")
            .labelNames("ip")
            .register();

    private static Summary processLatency = Summary.build().name("streaming_hive_process_files")
            .help("process latency")
            .labelNames("ip", "type")
            .register();


    private static Counter wroteRecords = Counter.build().name("streaming_hive_wrote_records")
            .help("wrote records")
            .labelNames("ip", "db", "table", "type")
            .register();

    public static void consumedMsg(Operation operation) {
        consumedMq.labels(NetworkUtils.localIp, operation.name())
                .inc();
    }

    public static Summary.Timer startInsert() {
        return processLatency.labels(NetworkUtils.localIp, "insert").startTimer();
    }

    public static Summary.Timer startUpdate() {
        return processLatency.labels(NetworkUtils.localIp, "update").startTimer();
    }

    public static void insertFiles(int increase) {
        processedInsertFiles.labels(NetworkUtils.localIp)
                .inc(increase);
    }

    public static void updateFiles(int increase) {
        processedUpdateFiles.labels(NetworkUtils.localIp)
                .inc(increase);
    }

    private static void wroteRecords(long increase, String db, String table, String op) {
        wroteRecords.labels(NetworkUtils.localIp, db, table, op).inc(increase);
    }

    public static void wroteRecords(MutateResult result, String db, String table) {
        if (0 != result.getInsert()) {
            wroteRecords(result.getInsert(), db, table, Operation.CREATE.name());
        }
        if (0 != result.getUpdate()) {
            wroteRecords(result.getUpdate(), db, table, Operation.UPDATE.name());
        }
        if (0 != result.getDelete()) {
            wroteRecords(result.getUpdate(), db, table, Operation.DELETE.name());
        }
    }

    public static void processedFiles(int size) {
        processedFiles.labels(NetworkUtils.localIp).inc(size);
    }
}
