package com.tree.finance.bigdata.hive.streaming.utils.record;

import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.LogicalDateObjectInspector;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.TimeStampObjectInspector;
import com.tree.finance.bigdata.hive.streaming.utils.common.StringUtils;
import com.tree.finance.bigdata.schema.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.tree.finance.bigdata.schema.SchemaConstants.*;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/28 13:40
 */
public class RecordUtils {

    public static String[] UPDATE_IDENTIFIER = ConfigHolder.getConfig().updateColIdentifier().split(",");
    public static String[] CREATE_IDENTIFIER = ConfigHolder.getConfig().createColIdentifier().split(",");
    public static String[] TIME_IDENTIFIER = ConfigHolder.getConfig().timeColIdentifier().split(",");
    
    private static Logger LOG = LoggerFactory.getLogger(RecordUtils.class);

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
            LOG.warn("found no update time column for table: {}, schema: {}, identifier: {}", table, fileSchema);
            tableToUpdateCol.put(table, "");
            return "";
        }
    }
    
    public static Long getFieldAsTimeMillis(String fieldName, GenericData.Record data) {
        if (StringUtils.isEmpty(fieldName)){
            return null;
        }
        Schema fieldSchema = data.getSchema().getField(FIELD_AFTER).schema().getField(fieldName).schema();
        Object value = ((GenericData.Record)data.get(FIELD_AFTER)).get(fieldName);

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
        return null;

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
