package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.processor.Processor;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DatabaseProcessor {
    private static Logger LOG = LoggerFactory.getLogger(DatabaseProcessor.class);
    private String db;
    private List<Processor> processors = new ArrayList<>();

    public DatabaseProcessor(String db) {
        this.db = db;
    }

    public void start() {
        for (int i=0; i < PioneerConfig.getDbCores(db); i++) {
            processors.add(new Processor(db, i));
        }
        processors.forEach( p -> p.start());
    }

    public void stop() {
        LOG.info("stopping database: {}", db);
        processors.forEach(p -> p.stop());
        LOG.info("stopped database: {}", db);
    }

}
