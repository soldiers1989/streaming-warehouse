package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import com.tree.finance.bigdata.hive.streaming.mutation.AvroStructField;
import com.tree.finance.bigdata.schema.LogicalType;
import com.tree.finance.bigdata.schema.SchemaConstants;
import com.tree.finance.bigdata.utils.common.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tree.finance.bigdata.schema.SchemaConstants.FIELD_AFTER;
import static com.tree.finance.bigdata.schema.SchemaConstants.PROP_KEY_LOGICAL_TYPE;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/3 15:16
 */
public class AvroObjectInspector extends StructObjectInspector {

    private List<StructField> fields = new ArrayList<>();
    private Map<String, StructField> name2FieldMap = new HashMap<>();
    private static Logger LOG = LoggerFactory.getLogger(AvroObjectInspector.class);
    private String db;
    private String table;
//    private HbaseUtils hbaseUtils;

    private Object2ObjectMap<String, RecordIdentifier> bizId2RecIdMap;

    public AvroObjectInspector(String db, String table, Schema schema, Object2ObjectMap bizId2RecIdMap) {
        this.db = db;
        this.table = table;
//        this.hbaseUtils = hbaseUtils;
        this.bizId2RecIdMap = bizId2RecIdMap;
        init(schema);
    }

    private void init(Schema schema) {

        int pos = 0;
        fields.add(new AvroStructField("recordId", pos++, new RecIdObjectInspector(
                schema.getField(SchemaConstants.FIELD_KEY).schema(), bizId2RecIdMap)));

        //数据字段
        ArrayList<StructField> colFields = new ArrayList();
        for (Schema.Field field : schema.getField(FIELD_AFTER).schema().getFields()) {
            StructField structField = convertToStruct(field, pos++);
            colFields.add(structField);
            name2FieldMap.put(field.name(), structField);
        }
        colFields.sort(Comparator.comparing(o -> o.getFieldName().toLowerCase()));
        fields.addAll(colFields);
    }

    private StructField convertToStruct(Schema.Field field, int pos) {
        return new AvroStructField(field.name(), pos, createObjectInspector(field.schema()));
    }

    private ObjectInspector createObjectInspector(Schema fieldShema) {

        if (!StringUtils.isEmpty(fieldShema.getProp(PROP_KEY_LOGICAL_TYPE))) {
            String logicalType = fieldShema.getProp(PROP_KEY_LOGICAL_TYPE);

            if (logicalType.equalsIgnoreCase(LogicalType.Date.value())) {
                return new LogicalDateObjectInspector();
            }
            if (logicalType.equalsIgnoreCase(LogicalType.Decimal.value())) {
                /*int scale = fieldShema.getJsonProp(PROP_KEY_SCALE).getIntValue();
                int precision = null == fieldShema.getJsonProp(PROP_KEY_PRECISION) ?
                        null : fieldShema.getJsonProp(PROP_KEY_PRECISION).getIntValue();
                return new LogicalDecimalObjectInspector(scale, precision);*/
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            }
            if (logicalType.equalsIgnoreCase(LogicalType.TimeStampMillis.value())) {
                return new TimeMillisObjectInspector();
            }
            if (logicalType.equalsIgnoreCase(LogicalType.ZonedTimestamp.value())) {
                return new TimeStampObjectInspector();
            }
        }

        switch (fieldShema.getType()) {
            case INT:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case LONG:
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case FLOAT:
                return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case BOOLEAN:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case STRING:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case BYTES:
                return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case RECORD:
                return new AvroObjectInspector(db, table, fieldShema, bizId2RecIdMap);
            case UNION: //仅存在两个元素，并且其中一个为NULL的场景
                for (Schema schema : fieldShema.getTypes()) {
                    if (!schema.getType().equals(Schema.Type.NULL)) {
                        return createObjectInspector(schema);
                    }
                }
            default
                    :
                LOG.error("Unsupported schema: ", fieldShema);
                throw new RuntimeException("Unsupported schema type: " + fieldShema.getType());
        }

    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
        return name2FieldMap.get(fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        GenericData.Record record = (GenericData.Record) data;
        if (fieldRef.getFieldID() == 0) {
            return ((GenericData.Record) data).get(SchemaConstants.FIELD_KEY);
        }
        return ((GenericData.Record) record.get(FIELD_AFTER)).get(fieldRef.getFieldName());
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        if (data == null) {
            return null;
        }
        List<Object> result = new ArrayList<>(fields.size());
        fields.forEach(f -> result.add(getStructFieldData(data, f)));
        return result;
    }

    @Override
    public String getTypeName() {
        return Category.STRUCT.name();
    }

    @Override
    public Category getCategory() {
        return Category.STRUCT;
    }

}
