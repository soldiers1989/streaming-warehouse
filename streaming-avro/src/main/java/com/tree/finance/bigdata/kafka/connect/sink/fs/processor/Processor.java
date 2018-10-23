package com.tree.finance.bigdata.kafka.connect.sink.fs.processor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.AvroSchemaClient;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.ValueConvertor;
import com.tree.finance.bigdata.kafka.connect.sink.fs.partition.PartitionHelper;
import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.Writer;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterFactory;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterManager;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.Operation;
import io.confluent.connect.avro.AvroConverter;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.tree.finance.bigdata.schema.SchemaConstants.*;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/6/29 11:33
 */
public class Processor {

    private Map config;
    private WriterFactory writerFactory;
    private PartitionHelper partitionHelper;
    private String sourceDb;
    private final String targetDB;
    private static Logger LOG = LoggerFactory.getLogger(Processor.class);
    private int processorId;
    private static Converter keyConverter = new AvroConverter();
    private static Converter valueConverter = new AvroConverter();
    private KafkaConsumer<byte[], byte[]> consumer;
    private Thread consumerThread;
    private volatile boolean stop;
    private CountDownLatch totalProcessors;
    private CountDownLatch self = new CountDownLatch(1);

    public Processor(String sourceDb, int processorId, CountDownLatch totalProcessors) {
        this.totalProcessors = totalProcessors;
        this.partitionHelper = new PartitionHelper();
        this.config = config;
        this.writerFactory = new WriterManager(sourceDb, processorId);
        this.sourceDb = sourceDb;
        this.targetDB = PioneerConfig.getHiveDestinationDbName(sourceDb);
        this.processorId = processorId;
        this.consumerThread = new Thread(this::run, "consumer-" + targetDB + "-" + processorId);
    }

    public void start() {
        try {
            writerFactory.init();
            //Kafka Consumer
            Map<String, Object> props = PioneerConfig.getKafkaConf();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, PioneerConfig.getConsumerGroupId(sourceDb));
            props.put("schema.registry.url", PioneerConfig.getConnectSchemaUrl());
            keyConverter.configure(props, true);
            valueConverter.configure(props, false);
            consumer = new KafkaConsumer(props);
            consumer.subscribe(Arrays.asList(PioneerConfig.getTopics(sourceDb)));
            consumerThread.start();
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    public void run() {
        while (!stop) {
            try {
                ConsumerRecords<byte[], byte[]> msgs = consumer.poll(10000l);
                for (ConsumerRecord<byte[], byte[]> msg : msgs) {
                    SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.key());
                    SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.value());
                    Long timestamp = msg.timestamp();
                    writeRecord(new SinkRecord(msg.topic(), msg.partition(),
                            keyAndSchema.schema(), keyAndSchema.value(),
                            valueAndSchema.schema(), valueAndSchema.value(),
                            msg.offset(),
                            timestamp,
                            msg.timestampType()));
                }
                consumer.commitSync();
            } catch (Throwable t) {
                LOG.error("error", t);
            }
        }
        self.countDown();
        totalProcessors.countDown();
        try {
            totalProcessors.await();
        }catch (InterruptedException e) {
            LOG.warn("error await other processors to stop");
        }
        //consumer can't be closed by other thread, and close consumer together to avoid consumer rebalance
        cleanResource();
        LOG.info("processor consumer thread stopped.");
    }

    private void writeRecord(SinkRecord sinkRecord) throws Exception {
        Writer<GenericData.Record> writer = null;
        try {
            WriterRef ref = getAvroWriterRef(sinkRecord);
            //when partition value is not set for update、delete operation
            if (null == ref) {
                return;
            }
            writer = writerFactory.getOrCreate(ref);
            //one connector on database
            writer.write(ValueConvertor.connectToGeneric(((AvroWriterRef) ref).getSchema(), sinkRecord));
        } finally {
            if (writer != null) {
                writer.unlock();
            }
        }
    }

    private WriterRef getAvroWriterRef(SinkRecord sinkRecord) throws Exception {
        Struct struct = (Struct) sinkRecord.value();
        if (struct == null) {
            return null;
        }
        Struct source = (Struct) struct.get(FIELD_SOURCE);
        //One processor processes only one database, we subscribe topics of the same db, so not validate database name
//        String dbName = DatabaseUtils.getConvertedDb(String.valueOf(source.get(FIELD_DB)));
        String tableName = String.valueOf(source.get(FIELD_TABLE));
        Struct after = (Struct) struct.get(FIELD_AFTER);
        if (after == null) {
            after = (Struct) struct.get(FIELD_BEFORE);
        }
        Integer version = sinkRecord.valueSchema().version();

        String op = ((Struct) sinkRecord.value()).getString(FIELD_OP);

        List<String> sourceParCols = partitionHelper.getSourcePartitionCols(new VersionedTable(targetDB, tableName, version), after);
        List<String> partitionVals = partitionHelper.buildYmdPartitionVals(sourceParCols.get(0), after);

        //if partition column has no value，then partition by current time for insert, ignore for update and delete.
        if (null == partitionVals) {
            if (Operation.CREATE.equals(Operation.forCode(op))) {
                partitionVals = new ArrayList<>();
                Calendar calendar = Calendar.getInstance();
                partitionVals.add(Integer.toString(calendar.get(Calendar.YEAR)));
                partitionVals.add(Integer.toString(calendar.get(Calendar.MONTH)));
                partitionVals.add(Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)));
            } else {
                LOG.warn("no partition value found, table: {}.{}, record: {}", targetDB, tableName, after);
                return null;
            }
        }

        VersionedTable versionedTable = new VersionedTable(targetDB, tableName, version);

        //not differentiate insert, update, delete
        return new AvroWriterRef(targetDB, tableName, partitionVals, 1, processorId,
                Operation.forCode(op), AvroSchemaClient.getSchema(versionedTable, sinkRecord), version);
    }

    public void stop() {
        LOG.info("stopping processor: {}-{}", targetDB, processorId);
        this.stop = true;
        try {
            this.self.await();
        }catch (InterruptedException e) {
            LOG.error("interupted awat count down");
        }
        this.writerFactory.close();
        LOG.info("stopped processor: {}-{}", targetDB, processorId);
    }

    private void cleanResource(){
        this.consumer.close();
    }
}
