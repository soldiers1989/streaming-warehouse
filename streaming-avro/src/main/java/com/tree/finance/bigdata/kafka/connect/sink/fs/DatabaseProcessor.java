package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.processor.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DatabaseProcessor {

    private static Logger LOG = LoggerFactory.getLogger(DatabaseProcessor.class);
    private String db;
    private List<Processor> processors = new ArrayList<>();
    private CountDownLatch totalProcessors;

    public DatabaseProcessor(String db) {
        this.db = db;
    }

    public void start() {
        this.totalProcessors = new CountDownLatch(PioneerConfig.getDbCores(db));
        for (int i=0; i < PioneerConfig.getDbCores(db); i++) {
            processors.add(new Processor(db, i, totalProcessors));
        }
        processors.forEach( p -> p.start());
    }

    public void stop() {
        LOG.info("stopping database: {}", db);
        ExecutorService poolExecutor = Executors.newFixedThreadPool(processors.size());
        processors.forEach(p -> poolExecutor.submit(new StopDbTask(p)));
        poolExecutor.shutdown();
        while (!poolExecutor.isTerminated()) {
            try {
                poolExecutor.awaitTermination(10, TimeUnit.SECONDS);
            }catch (InterruptedException e) {
                LOG.info("await to stop database: {}", db);
            }
        }
        try {
            totalProcessors.await();
        }catch (InterruptedException e) {
            LOG.error("error wait consumers to stop, db: {}", db);
        }

        LOG.info("stopped database: {}", db);
    }
    public class StopDbTask implements Runnable{
        private Processor processor;
        public StopDbTask(Processor processor) {
            this.processor = processor;
        }
        @Override
        public void run() {
            this.processor.stop();
        }
    }
}
