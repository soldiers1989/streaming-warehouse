package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import static com.tree.finance.bigdata.schema.SchemaConstants.*;


/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/28 16:15
 */
public class ValueConvertor {

    public static GenericData.Record connectToGeneric(Schema avroFileSchema, SinkRecord sinkRecord) {
        Struct value = (Struct) sinkRecord.value();
        Operation op = Operation.forCode(value.getString(FIELD_OP));
        Struct afterStruct;
        if (op.equals(Operation.DELETE)) {
            //delete only have delete value
            afterStruct = (Struct) value.get(FIELD_BEFORE);
        } else {
            afterStruct = (Struct) value.get(FIELD_AFTER);
        }
        GenericRecordBuilder afterBuilder = getRecordBuilder(avroFileSchema.getField(FIELD_AFTER).schema(), afterStruct);

        //id
        /*GenericRecordBuilder keyBuilder = new GenericRecordBuilder(avroFileSchema.getField(FIELD_KEY).schema());
        Struct key = (Struct) sinkRecord.key();
        key.schema().fields().forEach(field ->
                //change to lowercase
                keyBuilder.set(field.name().toLowerCase(), key.get(field))
        );*/
        GenericRecordBuilder keyBuilder = getRecordBuilder(avroFileSchema.getField(FIELD_KEY).schema(),
                (Struct) sinkRecord.key());

                GenericRecordBuilder builder = new GenericRecordBuilder(avroFileSchema);
        builder.set(FIELD_AFTER, afterBuilder.build());
        builder.set(FIELD_OP, op.code());
        builder.set(FIELD_KEY, keyBuilder.build());
        return builder.build();
    }

    private static GenericRecordBuilder getRecordBuilder(Schema avroSchema, Struct afterStruct) {
        GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);

        for (Field field : afterStruct.schema().fields()) {
            if (afterStruct.get(field.name()) == null) {
                continue;
            }
            if (null != field.schema().name()) {
                //Date LogicalType
                if (field.schema().name().equalsIgnoreCase(org.apache.kafka.connect.data.Date.LOGICAL_NAME)) {
                    if (field.schema().type().equals(Type.INT32)) {
                        Date date = ((Date) afterStruct.get(field.name()));
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                        //change to cst timezone
                        calendar.add(Calendar.HOUR, -8);
                        long unixMillis = calendar.getTimeInMillis();
                        builder.set(field.name().toLowerCase(), (unixMillis / 86400000));
                        continue;
                    }
                }

                //Date LogicalType
                if (field.schema().name().equalsIgnoreCase(Timestamp.LOGICAL_NAME)) {
                    if (field.schema().type().equals(Type.INT64)) {
                        Date date = ((Date) afterStruct.get(field.name()));
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                        //change to cst timezone
                        calendar.add(Calendar.HOUR, -8);
                        long unixMillis = calendar.getTimeInMillis();
                        builder.set(field.name().toLowerCase(), unixMillis);
                        continue;
                    }
                }

                //Decimal LogicalType
                if (field.schema().name().equalsIgnoreCase(Decimal.LOGICAL_NAME)) {
                    BigDecimal decimal = ((BigDecimal) afterStruct.get(field.name()));
                    if (null != decimal) {
//                    afterBuilder.set(field.name().toLowerCase(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
                        builder.set(field.name().toLowerCase(), decimal.doubleValue());
                    }
                    continue;
                }

            }

            //convert kafka connect's data type short to Avro int
            if (field.schema().type().equals(Type.INT8)) {
                if (null != afterStruct.get(field.name())) {
                    //change field name to lowercase letter
//                    afterBuilder.set(field.name().toLowerCase(), ((Short) afterStruct.get(field.name())).intValue());
                    builder.set(field.name().toLowerCase(), Long.valueOf((Integer) afterStruct.get(field.name())));
                }
            } else if (field.schema().type().equals(Type.INT16)) {
                if (null != afterStruct.get(field.name())) {
                    //change field name to lowercase letter
//                    afterBuilder.set(field.name().toLowerCase(), ((Short) afterStruct.get(field.name())).intValue());
                    builder.set(field.name().toLowerCase(), Long.valueOf((Short) afterStruct.get(field.name())));
                }
            } else if (field.schema().type().equals(Type.INT32)) {
                if (null != afterStruct.get(field.name())) {
                    //change field name to lowercase letter
                    builder.set(field.name().toLowerCase(), Long.valueOf((Integer) afterStruct.get(field.name())));
                }
            } else if (field.schema().type().equals(Type.FLOAT32)) {
                if (null != afterStruct.get(field.name())) {
                    //change field name to lowercase letter
                    builder.set(field.name().toLowerCase(), Double.valueOf((float) afterStruct.get(field.name())));
                }
            } else {
                //change field name to lowercase letter
                builder.set(field.name().toLowerCase(), afterStruct.get(field.name()));
            }
        }
        return builder;
    }
}
