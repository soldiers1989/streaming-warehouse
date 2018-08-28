package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.processor.Processor;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Map;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/6/29 10:19
 */
public class DfsSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(DfsSinkTask.class);

    private Processor processor;

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting task with config: {}", props);
        try {
            SinkConfig sinkConfig = new SinkConfig(props);
            DfsConfigHolder.init(sinkConfig);
            processor = new Processor(sinkConfig);
            processor.init();
        } catch (Exception e) {
            log.error("update hive config error,", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        if (log.isTraceEnabled()){
            final SinkRecord first = records.iterator().next();
            final int recordsCount = records.size();
            log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                    recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
        }
        try {
            processor.process(records);
        } catch (Exception e) {
            log.error("Write of {} records failed", records.size(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (this.processor != null){
            processor.stop();
        }
    }
}
