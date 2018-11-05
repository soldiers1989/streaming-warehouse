package com.tree.finance.bigdata.kafka.connect.sink.fs.processor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProcessorTest {
    String TOPIC = "loandb.t_cash_pos_new_history";
    private static Converter keyConverter = new AvroConverter();
    private static Converter valueConverter = new AvroConverter();
    static String SERVER_KAFKA = "kafka3:9092";
    static String schema = "http://application1:8081,http://application2:8081,http://application3:8081,http://application4:8081,http://application5:8081";

    @Before
    public void setUp() {

    }

    @Test
    public void getAvroRef() {

        System.setProperty("pioneer.conf.file", "/Users/jim/workspace/idea/bigdata/streaming-warehouse/streaming-avro/src/main/resources/test/pioneer.yaml");

        Processor processor = new Processor("loandb", 1, null);


        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_KAFKA);


        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("max.poll.records", 100);
        props.put("schema.registry.url", schema);

        keyConverter.configure(props, true);
        valueConverter.configure(props, false);

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(TOPIC));

        ExecutorService executor = Executors.newFixedThreadPool(8);

        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(10000);

        msgs.forEach( msg -> {
            SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.key());
            SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.value());
            Long timestamp = msg.timestamp();
            SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
                    keyAndSchema.schema(), keyAndSchema.value(),
                    valueAndSchema.schema(), valueAndSchema.value(),
                    msg.offset(),
                    timestamp,
                    msg.timestampType());
            try {
                WriterRef ref = processor.getAvroWriterRef(origRecord);
                System.out.println(ref.getPartitionName() + "\nrecord: " + origRecord.toString());
            }catch (Exception e) {
                e.printStackTrace();
            }
        });

        consumer.close();

    }
}
