package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/3 15:20
 */
public class AvroStructField implements StructField, Comparable<AvroStructField> {

    private String name;

    private int id;

    private ObjectInspector objectInspector;

    public AvroStructField(String name, int id, ObjectInspector objectInspector){
        this.name = name;
        this.id = id;
        this.objectInspector = objectInspector;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
        return objectInspector;
    }

    @Override
    public int getFieldID() {
        return id;
    }

    @Override
    public String getFieldComment() {
        return "";
    }

    @Override
    public int compareTo(AvroStructField o) {
        return name.compareTo(o.getFieldName());
    }
}
