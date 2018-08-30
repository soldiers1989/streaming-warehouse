package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/16 16:51
 */
public class SchemaTest {
    String str = "{\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Value\",\n" +
            "    \"namespace\": \"loandb.qs_repay_check_detail\",\n" +
            "    \"fields\": [\n" +
            "        {\n" +
            "            \"name\": \"l\",\n" +
            "            \"type\": \"long\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\": \"i\",\n" +
            "            \"type\": [\n" +
            "                \"null\",\n" +
            "                \"int\"\n" +
            "            ]\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\": \"s\",\n" +
            "            \"type\": \"string\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";
    @Test
    public void test() throws Exception{
        org.apache.avro.Schema schema = new Schema.Parser().parse(str);
        DataFileWriter writer = new DataFileWriter<>(new GenericDatumWriter<>()).create(schema, new File("/tmp/avro2.test"));
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("i", 33);
        builder.set("l", 22l);

        writer.append(builder.build());
        writer.close();
    }

}
