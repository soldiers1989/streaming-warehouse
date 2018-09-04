package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.sql.Date;

/**
 * @author Zhengsj
 * Description: Date is logical type in connect avro, it is stored as int value
 * Created in 2018/8/22 10:08
 */
public class LogicalDateObjectInspector extends AbstractPrimitiveJavaObjectInspector implements DateObjectInspector {

    public LogicalDateObjectInspector(){
        super(TypeInfoFactory.dateTypeInfo);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(Object o) {
        return new DateWritable(new Date(((int) o) * 86400000L));
    }

    public Date get(Object o) {
        return new Date(((int) o) * 86400000L);
    }

    public Date getPrimitiveJavaObject(Object o) {
        return o == null ? null : new Date(((int) o) * 86400000L);
    }

    public Date create(Object o) {
        return new Date(((int) o) * 86400000L);
    }

}
