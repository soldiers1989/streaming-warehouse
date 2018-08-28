package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;

import java.sql.Timestamp;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/20 19:25
 */
public class TimeStampObjectInspector extends JavaTimestampObjectInspector {

    public Timestamp reformat(Object o) {
        String s = o.toString();
        //2018-07-11T10:00:00+08:00
        if (s.charAt(10) == 'T') {
            s = s.replace('T', ' ');
        }
        if (s.contains("+")) {
            return Timestamp.valueOf(s.substring(0, s.indexOf('+')));
        }
        return Timestamp.valueOf(s);
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
