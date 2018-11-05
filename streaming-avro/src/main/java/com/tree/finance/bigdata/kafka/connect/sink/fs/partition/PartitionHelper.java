package com.tree.finance.bigdata.kafka.connect.sink.fs.partition;

import com.google.common.collect.Lists;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import com.tree.finance.bigdata.kafka.connect.sink.fs.utils.ConnectFieldUtils;
import com.tree.finance.bigdata.kafka.connect.sink.fs.utils.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/6/26 20:19
 * @Modified By:
 */
public class PartitionHelper {

    private static final Map<VersionedTable, List<String>> sourcePartitioinCols = new HashMap<>();

    public static final List<String> SINK_PARTITIONS_DEFAUL = Lists.newArrayList("p_y", "p_m", "p_d");

    String globalDefaults[];

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHelper.class);

    public PartitionHelper() {
        this.globalDefaults = PioneerConfig.getDefaultParClos().toLowerCase().split("\\|");
    }

    /**
     * @param sinkTable
     * @param struct    after field
     * @return
     * @throws Exception
     */
    public List<String> getSourcePartitionCols(VersionedTable sinkTable, Struct struct) throws Exception {
        if (sourcePartitioinCols.containsKey(sinkTable)) {
            return sourcePartitioinCols.get(sinkTable);
        }

        List<String> createStrs = Arrays.asList(globalDefaults[0].split(","));
        List<String> timeStrs = Arrays.asList(globalDefaults[1].split(","));

        List<Field> fields = struct.schema().fields();

        final List<String> partitionsFromDefault = new ArrayList<>();

        for (Field f : fields) {
            String fieldName = f.name().toLowerCase();
            boolean matchCreate = false;
            boolean matchTime = false;
            for (String createStr : createStrs) {
                if (fieldName.contains(createStr)) {
                    matchCreate = true;
                    break;
                }
            }
            if (!matchCreate) {
                continue;
            }
            for (String timeStr : timeStrs) {
                if (fieldName.contains(timeStr)) {
                    matchTime = true;
                    break;
                }
            }
            if (matchCreate && matchTime) {
                partitionsFromDefault.add(f.name());
                break;
            }
        }

        if (partitionsFromDefault.size() == 1) {
            sourcePartitioinCols.put(sinkTable, partitionsFromDefault);
            return partitionsFromDefault;
        } else {
            LOG.info("global partition config: {}", Arrays.toString(globalDefaults));
            LOG.error("table: {}, found illegal partition columns: [{}]", sinkTable, Arrays.toString(partitionsFromDefault.toArray()));
            throw new RuntimeException(sinkTable + " found " + partitionsFromDefault.size() + " partition columns");
        }
    }

    public static String getSinkPartitionName(List<String> partCols, List<String> vals) {
        StringBuilder name = new StringBuilder();

        for (int i = 0; i < partCols.size(); ++i) {
            if (i > 0) {
                name.append("/");
            }
            name.append(partCols.get(i));
            name.append('=');
            name.append(vals.get(i));
        }
        return name.toString();
    }

    public List<String> buildYmdPartitionVals(String sourceParCol, Struct value) {

        List<String> result = new ArrayList<>();
        Field field = value.schema().field(sourceParCol);

        //no partition value..
        if (value.get(field) == null) {
            return null;
        }

        if (null == field) {
            LOG.error("schema: [{}] not contains field: {}",
                    ConnectFieldUtils.toString(value.schema().fields()), sourceParCol);
            throw new RuntimeException("schema not contains field " + sourceParCol);
        }

        switch (field.schema().type()) {
            case INT32:
                int timeStamp = value.getInt32(sourceParCol);
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timeStamp * 1000);
                result.add(Integer.toString(calendar.get(Calendar.YEAR)));
                result.add(Integer.toString(calendar.get(Calendar.MONTH) + 1));
                result.add(Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)));
                return result;
            case STRING:
                String timeStr = value.getString(sourceParCol);
                return getYmdFromDateStr(timeStr, '-');
            case INT64:
                if (field.schema().name().equalsIgnoreCase(Timestamp.LOGICAL_NAME)) {
                    Calendar ts = Calendar.getInstance();
                    ts.setTime((Date) value.get(field));
                    ts.add(Calendar.HOUR, -8);
                    List<String> partitions = new ArrayList<>();
                    partitions.add(Integer.toString(ts.get(Calendar.YEAR)));
                    partitions.add(Integer.toString(ts.get(Calendar.MONTH) + 1));
                    partitions.add(Integer.toString(ts.get(Calendar.DAY_OF_MONTH)));
                    return partitions;
                }
            default:
                LOG.error("column: {}, get unsupported type: {}, value: {}, Struct: {}", field.name(), field.schema().type(), value.get(field), value.toString());
                throw new RuntimeException("unsupported partition type " + field.schema().type());
        }
    }

    public List<String> getYmdFromDateStr(String s, char c) {

        List<String> result = new ArrayList<>();

        if (s.length() < 12) {
            throw new RuntimeException("illegal date: " + s);
        }

        if (s.charAt(4) != c || s.charAt(7) != c) {
            throw new RuntimeException(s + " not contains tow separator: " + c);
        }
        result.add(s.substring(0, 4));

        String month = s.substring(5, 7);
        if (month.charAt(0) == '0') {
            month = month.substring(1, month.length());
        }
        result.add(month);

        String day = s.substring(8, 10);
        if (day.charAt(0) == '0') {
            day = day.substring(1, day.length());
        }
        result.add(day);

        return result;

        /*int startPos = 0;
        int found = 0;
        for (int i=0; i < s.length(); i++){
            if (s.charAt(i) == c){
                result.add(s.substring(startPos, i));
                startPos = i + 1;
                if (++found == 2){
                    return result;
                }
            }
        }
        if (found != 3){
            throw new RuntimeException("error get YMD from " + s);
        }
        return result;*/
    }

}
