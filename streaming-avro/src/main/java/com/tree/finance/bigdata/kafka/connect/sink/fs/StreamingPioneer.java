package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.processor.Processor;
import com.tree.finance.bigdata.service.Service;
import com.tree.finance.bigdata.service.ShutDownSocketListener;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamingPioneer implements Service {

    static Logger LOG = LoggerFactory.getLogger(StreamingPioneer.class);

    private List<DatabaseProcessor> dbProcessors = new ArrayList<>();

    private AtomicBoolean stopping = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        StreamingPioneer pioneer = new StreamingPioneer();
        pioneer.start();
    }

    private void start() throws Exception {

        //either stop by ShutDownSocketServer, or stop by shutdown hook
        ShutDownSocketListener shutDonwSocketServer = new ShutDownSocketListener(this, PioneerConfig.getShutDownPort());
        shutDonwSocketServer.init();

        Map dbConfs = PioneerConfig.getDBConfs();
        dbConfs.keySet().forEach(k -> {
            dbProcessors.add(new DatabaseProcessor(k.toString()));
        });
        dbProcessors.forEach(p -> p.start());

        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        LOG.info("startted StreamingPioneer");
    }

    public void stop() {
        if (stopping.compareAndSet(false, true)) {
            LOG.info("stopping StreamingPioneer...");
            dbProcessors.parallelStream().forEach(p -> p.stop());
            LOG.info("stopped StreamingPioneer");
            RabbitMqUtils.getInstance(PioneerConfig.getRabbitHost(), PioneerConfig.getRabbitPort()).close();
            LOG.info("cleaned resources");
        }
    }

}
