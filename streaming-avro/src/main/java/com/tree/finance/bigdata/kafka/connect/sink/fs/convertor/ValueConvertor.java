package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import static com.tree.finance.bigdata.schema.SchemaConstants.*;
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


/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/28 16:15
 */
public class ValueConvertor {

    public static GenericData.Record connectToGeneric(Schema schema, SinkRecord sinkRecord) {
        Struct value = (Struct) sinkRecord.value();
        Operation op = Operation.forCode(value.getString(FIELD_OP));
        Struct afterStruct;
        if (op.equals(Operation.DELETE)) {
            //delete only have delete value
            afterStruct = (Struct) value.get(FIELD_BEFORE);
        } else {
            afterStruct = (Struct) value.get(FIELD_AFTER);
        }
        GenericRecordBuilder afterBuilder = new GenericRecordBuilder(schema.getField(FIELD_AFTER).schema());

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
                        afterBuilder.set(field.name().toLowerCase(), (int) (unixMillis / 86400000L));
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
                        afterBuilder.set(field.name().toLowerCase(), unixMillis);
                        continue;
                    }
                }

                //Decimal LogicalType
                if (field.schema().name().equalsIgnoreCase(Decimal.LOGICAL_NAME)) {
                    BigDecimal decimal = ((BigDecimal) afterStruct.get(field.name()));
                    afterBuilder.set(field.name().toLowerCase(), ByteBuffer.wrap(decimal.unscaledValue().toByteArray()));
                    continue;
                }

            }

            //convert kafka connect's data type short to Avro int
            if (field.schema().type().equals(Type.INT16)) {
                if (null != afterStruct.get(field.name())) {
                    //change field name to lowercase letter
                    afterBuilder.set(field.name().toLowerCase(), ((Short) afterStruct.get(field.name())).intValue());
                }
            } else {
                //change field name to lowercase letter
                afterBuilder.set(field.name().toLowerCase(), afterStruct.get(field.name()));
            }
        }


        //id
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(schema.getField(FIELD_KEY).schema());
        Struct key = (Struct) sinkRecord.key();
        key.schema().fields().forEach(field ->
                //change to lowercase
                keyBuilder.set(field.name().toLowerCase(), key.get(field))
        );

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(FIELD_OP, op.code());
        builder.set(FIELD_AFTER, afterBuilder.build());
        builder.set(FIELD_KEY, keyBuilder.build());
        return builder.build();
    }
}
