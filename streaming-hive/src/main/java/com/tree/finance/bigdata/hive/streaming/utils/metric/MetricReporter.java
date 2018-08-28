package com.tree.finance.bigdata.hive.streaming.utils.metric;

import com.tree.finance.bigdata.utils.network.NetworkUtils;
import io.prometheus.client.Summary;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/26 19:09
 */
public class MetricReporter {

    private static Summary processedFiles = Summary.build().name("streaming_hive_processed_files")
            .help("avro files write to hive")
            .labelNames("ip", "type")
            .register();
    private static Summary processLatency = Summary.build().name("streaming_hive_process_files")
            .help("avro files write to hive")
            .labelNames("ip", "type")
            .register();

    public static void insertedBytes(long bytes) {
        processedFiles.labels(NetworkUtils.localIp, "insert").
                observe(bytes);
    }

    public static void updatedBytes(long bytes) {
        processedFiles.labels(NetworkUtils.localIp, "update").
                observe(bytes);
    }

    public static Summary.Timer startInsert() {
        return processLatency.labels(NetworkUtils.localIp, "insert").startTimer();
    }

    public static Summary.Timer startUpdate() {
        return processLatency.labels(NetworkUtils.localIp, "insert").startTimer();
    }

}
