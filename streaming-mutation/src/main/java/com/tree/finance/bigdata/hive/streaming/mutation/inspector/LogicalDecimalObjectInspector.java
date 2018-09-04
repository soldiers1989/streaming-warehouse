package com.tree.finance.bigdata.hive.streaming.mutation.inspector;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.nio.ByteBuffer;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/22 20:03
 */
public class LogicalDecimalObjectInspector extends WritableHiveDecimalObjectInspector {


    public LogicalDecimalObjectInspector(String scaleStr, String precisionStr) {
        super(new DecimalTypeInfo(Integer.valueOf(precisionStr), Integer.valueOf(scaleStr)));
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
        if (o == null) {
            return null;
        }
        ByteBuffer buffer = (ByteBuffer) o;
        return  new HiveDecimalWritable(buffer.array(), ((DecimalTypeInfo)this.typeInfo).getScale());
    }

}
