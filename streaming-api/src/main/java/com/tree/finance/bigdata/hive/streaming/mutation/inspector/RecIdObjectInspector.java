package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import com.tree.finance.bigdata.hive.streaming.mutation.AvroStructField;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 10:43
 */
public class RecIdObjectInspector extends StructObjectInspector {

    private List<StructField> fields = new ArrayList<>();
    private Map<String, StructField> name2Fields = new HashMap<>();
    private Schema idSchema;
    private String dbTableSuffix;

    private final String FIELD_NAME_ORIGINAL_TXN_FIELD = "originalTxnField";
    private final String FIELD_NAME_BUCKET_FIELD = "bucketField";
    private final String FIELD_NAME_ROWID = "rowIdField";

    private static Logger LOG = LoggerFactory.getLogger(RecIdObjectInspector.class);

    private byte[] defaultFamily = Bytes.toBytes("f");
    private byte[] defaultRowIdQualifier = Bytes.toBytes("recordId");

    private HbaseUtils hbaseUtils;

    RecIdObjectInspector(String db, String table, Schema idSchema, HbaseUtils hbaseUtils) {
        this.hbaseUtils = hbaseUtils;
        dbTableSuffix = "_" + db + "." + table;
        this.idSchema = idSchema;
        init();
    }

    private void init() {
        StructField originalTxnField = new AvroStructField(FIELD_NAME_ORIGINAL_TXN_FIELD, 0, PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fields.add(originalTxnField);
        name2Fields.put(FIELD_NAME_ORIGINAL_TXN_FIELD, originalTxnField);

        StructField bucketField = new AvroStructField(FIELD_NAME_BUCKET_FIELD, 1, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fields.add(bucketField);
        name2Fields.put(FIELD_NAME_BUCKET_FIELD, bucketField);

        StructField rowIdField = new AvroStructField(FIELD_NAME_ROWID, 1, PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        fields.add(rowIdField);
        name2Fields.put(FIELD_NAME_ROWID, rowIdField);

    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
        return name2Fields.get(fieldName);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        try {
            if (fieldRef.getFieldName().equals(FIELD_NAME_BUCKET_FIELD)) {
                return 0;
            }
            if (fieldRef.getFieldName().equals(FIELD_NAME_ROWID) || fieldRef.getFieldName().equals(FIELD_NAME_ORIGINAL_TXN_FIELD)) {
                String businessId = GenericRowIdUtils.assembleBuizId((GenericData.Record) data, idSchema);
                String rowId = hbaseUtils.getString(businessId, defaultFamily, defaultRowIdQualifier);
                String[] rowIds = rowId.split("_");
                if (fieldRef.getFieldName().equals(FIELD_NAME_ROWID)){
                    return Long.valueOf(rowIds[2]);
                } else if (fieldRef.getFieldName().equals(FIELD_NAME_ORIGINAL_TXN_FIELD)) {
                    return Long.valueOf(rowIds[0]);
                }
            }
            return null;
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        return null;
    }

    @Override
    public String getTypeName() {
        return null;
    }

    @Override
    public Category getCategory() {
        return null;
    }

}
