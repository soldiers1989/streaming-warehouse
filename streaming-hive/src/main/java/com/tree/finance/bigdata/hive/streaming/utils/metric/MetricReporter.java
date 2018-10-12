package com.tree.finance.bigdata.hive.streaming.utils.metric;

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
}
