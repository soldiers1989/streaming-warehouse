package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;

import java.sql.Timestamp;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/28 11:35
 */
public class TimeMillisObjectInspector extends JavaTimestampObjectInspector {

    public Timestamp reformat(Object o) {
        return new Timestamp((Long)o);
    }

    @Override
    public Timestamp get(Object o) {
        return reformat(o);
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
        return o == null ? null : reformat(o);
    }

    @Override
    public Object create(Timestamp value) {
        return reformat(value);
    }
}
