package com.tree.finance.bigdata.kafka.connect.sink.fs.convertor;

import com.tree.finance.bigdata.schema.LogicalType;
import com.tree.finance.bigdata.schema.SchemaConstants;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author ZhengShengJun
 * Created in 2018/5/25 10:42
 */
public class SchemaConvertor {

    private static final String NAMESPACE = "io.confluent.connect.avro";

    private static final String DEFAULT_SCHEMA_NAME = "ConnectDefault";

    private static final String CONNECT_NAME_PROP = "connect.name";
    private static final String CONNECT_DOC_PROP = "connect.doc";
    private static final String CONNECT_RECORD_DOC_PROP = "connect.record.doc";
    private static final String CONNECT_ENUM_DOC_PROP = "connect.enum.doc";
    private static final String CONNECT_VERSION_PROP = "connect.version";
    private static final String CONNECT_DEFAULT_VALUE_PROP = "connect.default";
    private static final String CONNECT_PARAMETERS_PROP = "connect.parameters";

    private static final String CONNECT_TYPE_PROP = "connect.type";

    private static final String CONNECT_TYPE_INT8 = "int8";
    private static final String CONNECT_TYPE_INT16 = "int16";

    private static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
    private static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";

    private static final String AVRO_PROP = "avro";
    private static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    private static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    private static final String AVRO_LOGICAL_DATE = "date";
    private static final String AVRO_LOGICAL_DECIMAL = "decimal";
    private static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    private static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    private static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
    //  static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;
    private static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 38;

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private static final String MAP_ENTRY_TYPE_NAME = "MapEntry";

    private static final HashMap<String, LogicalTypeConverter> TO_AVRO_LOGICAL_CONVERTERS
            = new HashMap<>();

    private static boolean connectMetaData = false;
    private static boolean enhancedSchemaSupport = false;

    static {
        TO_AVRO_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof BigDecimal)) {
                throw new DataException(
                        "Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
            }
            return Decimal.fromLogical(schema, (BigDecimal) value);
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value) -> {
            if (!(value instanceof Date)) {
                throw new DataException(
                        "Invalid type for Date, expected Date but was " + value.getClass());
            }
            return Date.fromLogical(schema, (java.util.Date) value);
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema , value) -> {
                if (!(value instanceof Date)) {
                    throw new DataException(
                            "Invalid type for Time, expected Date but was " + value.getClass());
                }
                return Time.fromLogical(schema, (java.util.Date) value);
        });

        TO_AVRO_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
                if (!(value instanceof Date)) {
                    throw new DataException(
                            "Invalid type for Timestamp, expected Date but was " + value.getClass());
                }
                return Timestamp.fromLogical(schema, (java.util.Date) value);
        });
    }

    public static org.apache.avro.Schema fromConnectSchema(org.apache.kafka.connect.data.Schema schema,
                                                           Map<org.apache.kafka.connect.data.Schema, Schema> schemaMap,
                                                           boolean ignoreOptional) {
        if (schema == null) {
            return null;
        }

        String namespace = NAMESPACE;
        String name = DEFAULT_SCHEMA_NAME;

        if (schema.name() != null) {
            String[] split = splitName(schema.name());
            namespace = split[0];
            name = split[1];
        }

        // Extra type annotation information for otherwise lossy conversions
        String connectType = null;

        org.apache.avro.Schema baseSchema;
        
        switch (schema.type()) {
            case INT8:
                connectType = CONNECT_TYPE_INT8;
//                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case INT16:
                connectType = CONNECT_TYPE_INT16;
//                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case INT32:
//                baseSchema = org.apache.avro.SchemaBuilder.builder().intType();
                baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case INT64:
                baseSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case FLOAT32:
//                baseSchema = org.apache.avro.SchemaBuilder.builder().floatType();
                baseSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
                break;
            case FLOAT64:
                baseSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
                break;
            case BOOLEAN:
                baseSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
                break;
            case STRING:
                if (enhancedSchemaSupport && schema.parameters() != null
                        && schema.parameters().containsKey(AVRO_TYPE_ENUM)) {
                    List<String> symbols = new ArrayList<>();
                    for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                        if (entry.getKey().startsWith(AVRO_TYPE_ENUM + ".")) {
                            symbols.add(entry.getValue());
                        }
                    }
                    baseSchema =
                            org.apache.avro.SchemaBuilder.builder().enumeration(
                                    schema.parameters().get(AVRO_TYPE_ENUM))
                                    .doc(schema.parameters().get(CONNECT_ENUM_DOC_PROP))
                                    .symbols(symbols.toArray(new String[symbols.size()])); }
                else {
                    baseSchema = org.apache.avro.SchemaBuilder.builder().stringType();
                    if (null != schema.name() && schema.name().equals(LogicalType.ZonedTimestamp.value())) {
                        baseSchema.addProp(SchemaConstants.PROP_KEY_LOGICAL_TYPE, schema.name());
                    }
                }
                break;
            case BYTES:
                baseSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    /*int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                    baseSchema.addProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP, new IntNode(scale));
                    if (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
                        String precisionValue = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
                        int precision = Integer.parseInt(precisionValue);
                        baseSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP, new IntNode(precision));
                    } else {
                        baseSchema
                                .addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP,
                                        new IntNode(CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT));
                    }*/
                    baseSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
                }
                break;
            case ARRAY:
                baseSchema = org.apache.avro.SchemaBuilder.builder().array()
                        .items(fromConnectSchema(schema.valueSchema()));
                break;
            case MAP:
                // Avro only supports string keys, so we match the representation when possible, but
                // otherwise fall back on a record representation
                if (schema.keySchema().type() == org.apache.kafka.connect.data.Schema.Type.STRING && !schema.keySchema().isOptional()) {
                    baseSchema = org.apache.avro.SchemaBuilder.builder()
                            .map().values(fromConnectSchema(schema.valueSchema()));
                } else {
                    // Special record name indicates format
                    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler
                            = org.apache.avro.SchemaBuilder.builder()
                            .array().items()
                            .record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields();
                    addAvroRecordField(fieldAssembler, KEY_FIELD, schema.keySchema(), schemaMap);
                    addAvroRecordField(fieldAssembler, VALUE_FIELD, schema.valueSchema(), schemaMap);
                    baseSchema = fieldAssembler.endRecord();
                }
                break;
            case STRUCT:
                if (AVRO_TYPE_UNION.equals(schema.name())) {
                    List<Schema> unionSchemas = new ArrayList<>();
                    if (schema.isOptional()) {
                        unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    }
                    for (Field field : schema.fields()) {
                        unionSchemas.add(fromConnectSchema(nonOptional(field.schema()), schemaMap, true));
                    }
                    baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else if (schema.isOptional()) {
                    List<Schema> unionSchemas = new ArrayList<>();
                    unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    unionSchemas.add(fromConnectSchema(nonOptional(schema), schemaMap, false));
                    baseSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else {
                    String doc = schema.parameters() != null
                            ? schema.parameters().get(CONNECT_RECORD_DOC_PROP)
                            : null;
                    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema>
                            fieldAssembler =
                            org.apache.avro.SchemaBuilder.record(name != null ? name : DEFAULT_SCHEMA_NAME)
                                    .namespace(namespace)
                                    .doc(doc).fields();
                    for (Field field : schema.fields()) {
                        addAvroRecordField(fieldAssembler, field.name(), field.schema(), schemaMap);
                    }
                    baseSchema = fieldAssembler.endRecord();
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + schema.type());
        }

        org.apache.avro.Schema finalSchema = baseSchema;
        if (!baseSchema.getType().equals(org.apache.avro.Schema.Type.UNION)) {
            if (connectMetaData) {
                if (schema.doc() != null) {
                    baseSchema.addProp(CONNECT_DOC_PROP, schema.doc());
                }
                if (schema.version() != null) {
                    baseSchema.addProp(CONNECT_VERSION_PROP,
                            JsonNodeFactory.instance.numberNode(schema.version()));
                }
                if (schema.parameters() != null) {
                    baseSchema.addProp(CONNECT_PARAMETERS_PROP, parametersFromConnect(schema.parameters()));
                }
                if (schema.defaultValue() != null) {
                    baseSchema.addProp(CONNECT_DEFAULT_VALUE_PROP,
                            defaultValueFromConnect(schema, schema.defaultValue()));
                }
                if (schema.name() != null) {
                    baseSchema.addProp(CONNECT_NAME_PROP, schema.name());
                }
                // Some Connect types need special annotations to preserve the types accurate due to
                // limitations in Avro. These types get an extra annotation with their Connect type
                if (connectType != null) {
                    baseSchema.addProp(CONNECT_TYPE_PROP, connectType);
                }
            }

            // Only Avro named types (record, enum, fixed) may contain namespace + name. Only Connect's
            // struct converts to one of those (record), so for everything else that has a name we store
            // the full name into a special property. For uniformity, we also duplicate this info into
            // the same field in records as well even though it will also be available in the namespace()
            // and name().
            if (schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DECIMAL);
                } else if (Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIME_MILLIS);
                } else if (Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_TIMESTAMP_MILLIS);
                } else if (Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    baseSchema.addProp(AVRO_LOGICAL_TYPE_PROP, AVRO_LOGICAL_DATE);
                }
            }

            if (schema.parameters() != null) {
                for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                    if (entry.getKey().startsWith(AVRO_PROP)) {
                        baseSchema.addProp(entry.getKey(), entry.getValue());
                    }
                }
            }

            // Note that all metadata has already been processed and placed on the baseSchema because we
            // can't store any metadata on the actual top-level schema when it's a union because of Avro
            // constraints on the format of schemas.
            if (!ignoreOptional) {
                if (schema.isOptional()) {
                    if (schema.defaultValue() != null) {
                        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
                                .type(baseSchema).and()
                                .nullType()
                                .endUnion();
                    } else {
                        finalSchema = org.apache.avro.SchemaBuilder.builder().unionOf()
                                .nullType().and()
                                .type(baseSchema)
                                .endUnion();
                    }
                }
            }
        }

        if (!schema.isOptional()) {
            schemaMap.put(schema, finalSchema);
        }
        return finalSchema;
    }

    /**
     * Split a full dotted-syntax name into a namespace and a single-component name.
     */
    private static String[] splitName(String fullName) {
        String[] result = new String[2];
        int indexLastDot = fullName.lastIndexOf('.');
        if (indexLastDot >= 0) {
            result[0] = fullName.substring(0, indexLastDot);
            result[1] = fullName.substring(indexLastDot + 1);
        } else {
            result[0] = null;
            result[1] = fullName;
        }
        return result;
    }

    private static void addAvroRecordField(
            org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fieldAssembler,
            String fieldName, org.apache.kafka.connect.data.Schema fieldSchema, Map<org.apache.kafka.connect.data.Schema, Schema> schemaMap) {
        //change to lowercase letters
        org.apache.avro.SchemaBuilder.GenericDefault<org.apache.avro.Schema> fieldAvroSchema
                = fieldAssembler.name(fieldName.toLowerCase()).doc(fieldSchema.doc()).type(fromConnectSchema(fieldSchema,
                schemaMap));
        if (fieldSchema.defaultValue() != null) {
            fieldAvroSchema.withDefault(defaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()));
        } else {
            if (fieldSchema.isOptional()) {
                fieldAvroSchema.withDefault(JsonNodeFactory.instance.nullNode());
            } else {
                fieldAvroSchema.noDefault();
            }
        }
    }

    public static org.apache.avro.Schema fromConnectSchema(org.apache.kafka.connect.data.Schema schema) {
        return fromConnectSchema(schema, new HashMap<org.apache.kafka.connect.data.Schema, Schema>());
    }

    public static org.apache.avro.Schema fromConnectSchema(org.apache.kafka.connect.data.Schema schema,
                                                           Map<org.apache.kafka.connect.data.Schema, Schema> schemaMap) {
        return fromConnectSchema(schema, schemaMap, false);
    }

    // Convert default values from Connect data format to Avro's format, which is an
    // org.codehaus.jackson.JsonNode. The default value is provided as an argument because even
    // though you can get a default value from the schema, default values for complex structures need
    // to perform the same translation but those defaults will be part of the original top-level
    // (complex type) default value, not part of the child schema.
    private static JsonNode defaultValueFromConnect(org.apache.kafka.connect.data.Schema schema, Object value) {
        try {
            // If this is a logical type, convert it from the convenient Java type to the underlying
            // serializeable format
            Object defaultVal = value;
            if (schema != null && schema.name() != null) {
                LogicalTypeConverter logicalConverter = TO_AVRO_LOGICAL_CONVERTERS.get(schema.name());
                if (logicalConverter != null && value != null) {
                    defaultVal = logicalConverter.convert(schema, value);
                }
            }

            switch (schema.type()) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode((Byte) defaultVal);
                case INT16:
                    return JsonNodeFactory.instance.numberNode((Short) defaultVal);
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) defaultVal);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) defaultVal);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) defaultVal);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) defaultVal);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) defaultVal);
                case STRING:
                    return JsonNodeFactory.instance.textNode((String) defaultVal);
                case BYTES:
                    if (defaultVal instanceof byte[]) {
                        return JsonNodeFactory.instance.binaryNode((byte[]) defaultVal);
                    } else {
                        return JsonNodeFactory.instance.binaryNode(((ByteBuffer) defaultVal).array());
                    }
                case ARRAY: {
                    ArrayNode array = JsonNodeFactory.instance.arrayNode();
                    for (Object elem : (Collection<Object>) defaultVal) {
                        array.add(defaultValueFromConnect(schema.valueSchema(), elem));
                    }
                    return array;
                }
                case MAP:
                    if (schema.keySchema().type() == org.apache.kafka.connect.data.Schema.Type.STRING && !schema.keySchema().isOptional()) {
                        ObjectNode node = JsonNodeFactory.instance.objectNode();
                        for (Map.Entry<String, Object> entry : ((Map<String, Object>) defaultVal).entrySet()) {
                            JsonNode entryDef = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            node.put(entry.getKey(), entryDef);
                        }
                        return node;
                    } else {
                        ArrayNode array = JsonNodeFactory.instance.arrayNode();
                        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) defaultVal).entrySet()) {
                            JsonNode keyDefault = defaultValueFromConnect(schema.keySchema(), entry.getKey());
                            JsonNode valDefault = defaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            ArrayNode jsonEntry = JsonNodeFactory.instance.arrayNode();
                            jsonEntry.add(keyDefault);
                            jsonEntry.add(valDefault);
                            array.add(jsonEntry);
                        }
                        return array;
                    }
                case STRUCT: {
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    Struct struct = ((Struct) defaultVal);
                    for (Field field : (schema.fields())) {
                        JsonNode fieldDef = defaultValueFromConnect(field.schema(), struct.get(field));
                        node.put(field.name(), fieldDef);
                    }
                    return node;
                }
                default:
                    throw new DataException("Unknown schema type:" + schema.type());
            }
        } catch (ClassCastException e) {
            throw new DataException("Invalid type used for default value of "
                    + schema.type()
                    + " field: "
                    + schema.defaultValue().getClass());
        }
    }

    private interface LogicalTypeConverter {

        Object convert(org.apache.kafka.connect.data.Schema schema, Object value);
    }

    private static JsonNode parametersFromConnect(Map<String, String> params) {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static org.apache.kafka.connect.data.Schema nonOptional(org.apache.kafka.connect.data.Schema schema) {
        return new ConnectSchema(schema.type(), false, schema.defaultValue(), schema.name(),
                schema.version(), schema.doc(),
                schema.parameters(),
                fields(schema),
                keySchema(schema),
                valueSchema(schema));
    }

    public static List<Field> fields(org.apache.kafka.connect.data.Schema schema) {
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        if (org.apache.kafka.connect.data.Schema.Type.STRUCT.equals(type)) {
            return schema.fields();
        } else {
            return null;
        }
    }

    public static org.apache.kafka.connect.data.Schema keySchema(org.apache.kafka.connect.data.Schema schema) {
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        if (org.apache.kafka.connect.data.Schema.Type.MAP.equals(type)) {
            return schema.keySchema();
        } else {
            return null;
        }
    }

    public static org.apache.kafka.connect.data.Schema valueSchema(org.apache.kafka.connect.data.Schema schema) {
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        if (org.apache.kafka.connect.data.Schema.Type.MAP.equals(type) || org.apache.kafka.connect.data.Schema.Type.ARRAY.equals(type)) {
            return schema.valueSchema();
        } else {
            return null;
        }
    }
}
