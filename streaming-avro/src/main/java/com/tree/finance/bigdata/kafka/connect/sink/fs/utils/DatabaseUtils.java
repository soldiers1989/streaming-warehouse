package com.tree.finance.bigdata.kafka.connect.sink.fs.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/27 18:48
 */
public class DatabaseUtils {

    private static Map<String, String> converted = new ConcurrentHashMap<>();

    private static Logger LOG = LoggerFactory.getLogger(DatabaseUtils.class);

    public static String getConvertedDb(String db) {
        if (converted.containsKey(db)) {
            return converted.get(db);
        } else {
            String regex = "([a-zA-Z\\-_]+)([0-9]+)";
            Pattern pattern = Pattern.compile(regex);
            Matcher m = pattern.matcher(db);
            if (m.find()) {
                LOG.info("converted database name from {} to {}", db, m.group(1));
                converted.put(db, m.group(1));
            } else {
                converted.put(db, db);
            }
            return converted.get(db);
        }
    }
}
