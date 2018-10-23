package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.tree.finance.bigdata.schema.SchemaConstants.*;

/**
 * @author Zhengsj
 * Description
 * reated in 2018/6/27 16:28
 */
public class AvroSchemaClient {


    private static Logger LOG = LoggerFactory.getLogger(AvroSchemaClient.class);

    private static Map<VersionedTable, Schema> schemaCache = new ConcurrentHashMap<>();

    public static Schema getSchema(VersionedTable versionedTable, SinkRecord sinkRecord) {
        try {
            if (schemaCache.containsKey(versionedTable)) {
                return schemaCache.get(versionedTable);
            } else {
                //必须保持一致性
                synchronized (schemaCache) {
                    if (!schemaCache.containsKey(versionedTable)) {
                        Schema schema = extractAvroSchema(versionedTable, sinkRecord);
                        schemaCache.put(versionedTable, schema);
                        LOG.info("created table schema table: {}, schema: {}", versionedTable, schema);
                    }
                }
            }
            return schemaCache.get(versionedTable);
        } catch (Exception e) {
            LOG.error("failed to get schema", e);
            throw new RuntimeException("schema not exists for table: " + versionedTable);
        }
    }

    private static Schema extractAvroSchema(VersionedTable sinkTable, SinkRecord sinkRecord) {


        Struct value = (Struct) sinkRecord.value();
        Operation op = Operation.forCode(value.getString(FIELD_OP));

        org.apache.kafka.connect.data.Schema afterSchema;
        if (op.equals(Operation.DELETE)) {
            //delete only have before value
            afterSchema = sinkRecord.valueSchema().schema().field(FIELD_BEFORE).schema();
        } else {
            afterSchema = sinkRecord.valueSchema().schema().field(FIELD_AFTER).schema();
        }


        Schema afterAvroSchema = SchemaConvertor.fromConnectSchema(afterSchema).getTypes().get(1);
        org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema>
                fieldAssembler =
                org.apache.avro.SchemaBuilder.record(sinkTable.getTable())
                        .namespace(sinkTable.getDb())
                        .fields();
        fieldAssembler.name(FIELD_AFTER).type(afterAvroSchema).noDefault();
        fieldAssembler.name(FIELD_OP).type(SchemaConvertor.fromConnectSchema(sinkRecord.valueSchema().schema().field("op").schema())).noDefault();
        fieldAssembler.name(FIELD_KEY).type(SchemaConvertor.fromConnectSchema(sinkRecord.keySchema())).noDefault();

        Schema finalSchema = fieldAssembler.endRecord();
        LOG.info("table: {} schema: {}", sinkTable, finalSchema);
        return finalSchema;
    }

}
