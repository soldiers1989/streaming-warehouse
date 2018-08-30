package com.tree.finance.bigdata.kafka.connect.sink.fs;

import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.AvroSchemaClient;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.ValueConvertor;
import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import io.confluent.connect.avro.AvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/3 16:20
 */
public class TestSinkWriteToAvro {

    private static Converter keyConverter = new AvroConverter();
    private static Converter valueConverter = new AvroConverter();

    static String SERVER_KAFKA = "kafka2:9092";
    //loandb.fee_recharge_billing-value, payment.tran_card_category-value, t_channel_app-value, t_user_credit_risk_history-value, basisdata.wy_moxie_overdue_information-value
    static String TOPIC = "rules_engine.t_rule_flow_instance_parameter";
    static String schema = "http://application1:8081,http://application2:8081,http://application3:8081,http://application4:8081,http://application5:8081";

//    static String SERVER_KAFKA = "localhost:9092";
//    static String TOPIC = "mysql_localdb.test.test";
//    static String schema = "http://localhost:8081";

    public static void main(String[] args) throws Exception {

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-sink-234612");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_KAFKA);


        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("schema.registry.url", schema);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "4096");

        keyConverter.configure(props, true);
        valueConverter.configure(props, false);

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(TOPIC));
        ConsumerRecords<byte[], byte[]> msgs = consumer.poll(10000);

        convertMessages(msgs);

        consumer.close();
    }

    private static void convertMessages(ConsumerRecords<byte[], byte[]> msgs) throws Exception {

        for (ConsumerRecord<byte[], byte[]> msg : msgs) {
            SchemaAndValue keyAndSchema = keyConverter.toConnectData(msg.topic(), msg.key());
            SchemaAndValue valueAndSchema = valueConverter.toConnectData(msg.topic(), msg.value());
            Long timestamp = msg.timestamp();
            SinkRecord origRecord = new SinkRecord(msg.topic(), msg.partition(),
                    keyAndSchema.schema(), keyAndSchema.value(),
                    valueAndSchema.schema(), valueAndSchema.value(),
                    msg.offset(),
                    timestamp,
                    msg.timestampType());

            Schema avroSchema = AvroSchemaClient.getSchema(new VersionedTable("test", "par_test", 1),origRecord);

            System.out.println("avro-schema: ");
            System.out.println(avroSchema);

            GenericData.Record record = ValueConvertor.connectToGeneric(avroSchema, origRecord);

            DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(
                    new GenericDatumWriter<>());
            dataFileWriter.create(avroSchema, new File("/tmp/tmp2.avro"));
            dataFileWriter.append(record);
            dataFileWriter.close();
            break;

        }

    }

    public static GenericData.Record connectToGeneric(Schema schema, Struct value){
        Struct afterStruct = (Struct) value.get("after");
        GenericRecordBuilder afterBuilder = new GenericRecordBuilder(schema.getField("after").schema());
        for (Schema.Field field : schema.getField("after").schema().getFields()){
            afterBuilder.set(field, afterStruct.get(field.name()));
        }
        GenericData.Record afterRec =  afterBuilder.build();
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("op", value.get("op"));
        builder.set("after", afterRec);
        return builder.build();
    }

}
