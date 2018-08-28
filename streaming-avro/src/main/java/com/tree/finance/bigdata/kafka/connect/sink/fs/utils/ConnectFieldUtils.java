package com.tree.finance.bigdata.kafka.connect.sink.fs.utils;

import org.apache.kafka.connect.data.Field;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/20 15:20
 */
public class ConnectFieldUtils {

    public static String toString(List<Field> fields){
        if (null == fields || fields.isEmpty()) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        fields.forEach(s -> sb.append(s).append(','));
        return sb.toString();
    }

}
