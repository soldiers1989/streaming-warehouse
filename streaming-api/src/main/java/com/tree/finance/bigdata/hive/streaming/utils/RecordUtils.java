package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.constants.Constants;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.LogicalDateObjectInspector;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.TimeStampObjectInspector;
import com.tree.finance.bigdata.schema.LogicalType;
import com.tree.finance.bigdata.utils.common.StringUtils;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tree.finance.bigdata.hive.streaming.constants.Constants.*;
import static com.tree.finance.bigdata.schema.SchemaConstants.FIELD_AFTER;
import static com.tree.finance.bigdata.schema.SchemaConstants.PROP_KEY_LOGICAL_TYPE;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/28 13:40
 */
public class RecordUtils {

    public static String[] UPDATE_IDENTIFIER;
    public static String[] CREATE_IDENTIFIER;
    public static String[] TIME_IDENTIFIER;
    private static Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

    static {
        try {
            UPDATE_IDENTIFIER = ConfigFactory.getConfig().getProperty(COLUMN_UPDATE_IDENTIFIER).split(",");
            TIME_IDENTIFIER = ConfigFactory.getConfig().getProperty(COLUMN_TIME_IDENTIFIER).split(",");
            CREATE_IDENTIFIER = ConfigFactory.getConfig().getProperty(COLUMN_CREATE_IDENTIFIER).split(",");
        }catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }
    
    public static Map<String, String> tableToUpdateCol = new HashMap<>();

    public static Map<String, String> tableToCreateTimeCol = new HashMap<>();

    public static String getUpdateCol(String table, Schema fileSchema) {
        if (tableToUpdateCol.containsKey(table)) {
            return tableToUpdateCol.get(table);
        }else {
            List<Schema.Field> fields = fileSchema.getField(FIELD_AFTER).schema().getFields();

            for (Schema.Field f : fields) {
                String fieldName = f.name().toLowerCase();
                boolean matchUpdate = false;
                boolean matchTime = false;
                for (String updateStr : UPDATE_IDENTIFIER) {
                    if (fieldName.contains(updateStr)) {
                        matchUpdate = true;
                        break;
                    }
                }
                if (! matchUpdate){
                    continue;
                }
                for (String timeStr : TIME_IDENTIFIER) {
                    if (fieldName.contains(timeStr)) {
                        matchTime = true;
                        break;
                    }
                }
                if (matchUpdate && matchTime) {
                    tableToUpdateCol.put(table, f.name()) ;
                    return f.name();
                }
            }
            //put empty avoid retry 
            LOG.warn("found no update time column for table: {}, schema: {}", table, fileSchema);
            tableToUpdateCol.put(table, "");
            return "";
        }
    }
    
    public static Long getFieldAsTimeMillis(String fieldName, GenericData.Record data) {
        if (StringUtils.isEmpty(fieldName)){
            LOG.error("update time not found in record field: {}, value: {}", fieldName, data);
            throw new RuntimeException("update time not found in record value");
        }
        Schema fieldSchema = data.getSchema().getField(FIELD_AFTER).schema().getField(fieldName).schema();
        Object value = ((GenericData.Record)data.get(FIELD_AFTER)).get(fieldName);
        if (fieldSchema.getType().equals(Schema.Type.UNION)) {
            fieldSchema = fieldSchema.getTypes().get(1);
        }


        if (!StringUtils.isEmpty(fieldSchema.getProp(PROP_KEY_LOGICAL_TYPE))) {
            String logicalType = fieldSchema.getProp((PROP_KEY_LOGICAL_TYPE));

            if (logicalType.equalsIgnoreCase(LogicalType.Date.value())) {
                return new LogicalDateObjectInspector().create(value).getTime();
            }
            if (logicalType.equalsIgnoreCase(LogicalType.TimeStampMillis.value())) {
                return (Long) value;
            }
            if (logicalType.equalsIgnoreCase(LogicalType.ZonedTimestamp.value())) {
                return new TimeStampObjectInspector().get(value).getTime();
            }
        }

        LOG.error("update column not in support type, column: {}, schema: {}", fieldName, fieldSchema);
        throw new RuntimeException("update column not found");

    }

    public static String getCreateTimeCol(String table, Collection<String> fieldNames) {

        if (table.equalsIgnoreCase("loandb.lp_activity")){
            System.out.println();
        }

        if (tableToCreateTimeCol.containsKey(table)) {
            return tableToCreateTimeCol.get(table);
        }else {
            for (String f : fieldNames) {
                String fieldName = f.toLowerCase();
                boolean matchUpdate = false;
                boolean matchTime = false;
                for (String createStr : CREATE_IDENTIFIER) {
                    if (fieldName.contains(createStr)) {
                        matchUpdate = true;
                        break;
                    }
                }
                if (! matchUpdate){
                    continue;
                }
                for (String timeStr : TIME_IDENTIFIER) {
                    if (fieldName.contains(timeStr)) {
                        matchTime = true;
                        break;
                    }
                }
                if (matchUpdate && matchTime) {
                    tableToCreateTimeCol.put(table, f) ;
                    return f;
                }
            }
            //put empty avoid retry
            LOG.warn("found no update time column for table: {}, schema: {}, identifier: {}", table, fieldNames);
            tableToCreateTimeCol.put(table, "");
            return "";
        }
    }
    
    
}
