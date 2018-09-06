package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/11 10:58
 */
public class GenericRowIdUtils {

    private static Logger LOG = LoggerFactory.getLogger(GenericRowIdUtils.class);

    public static final String FILTER = "__dbz__";

    public static String assembleBuizId(GenericRecord keyRecord, Schema idSchema){
        StringBuilder sb = new StringBuilder();
        for (Schema.Field field : idSchema.getFields()){
            if (field.name().contains(FILTER)){
                continue;
            }
            sb.append(keyRecord.get(field.name())).append('_');
        }
        if (sb.length() > 1){
            return sb.deleteCharAt(sb.length() -1 ).toString();
        }else {
            //todo 无主键表如何处理
            LOG.error("no primary key found");
            throw new RuntimeException("not primary key found");
        }
    }

}
