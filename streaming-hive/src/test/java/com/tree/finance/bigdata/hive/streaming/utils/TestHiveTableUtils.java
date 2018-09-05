package com.tree.finance.bigdata.hive.streaming.utils;

import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveDDLUtils;
import org.junit.Test;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 13:13
 */
public class TestHiveTableUtils {

    @Test
    public void createHiveTblDDLFromMysql() throws Exception {
        Properties properties = new Properties();
        properties.load(TestHiveTableUtils.class.getResourceAsStream("/mysql.database.properties"));
        new HiveDDLUtils(properties).createAllTables();
    }

    @Test
    public void testRegex() {
        String a = null;
        StringBuilder sb = new StringBuilder().append(a);
        System.out.println(sb.toString());
        String regex = "(\\w+)(\\([0-9]+\\))";
        Pattern pattern = Pattern.compile(regex);
        Matcher m = pattern.matcher("table(3)");

        if (m.find()){
            System.out.println(m.group(1));
        }


        //double(2,3)
        String regex2 = "(\\w+)(\\([0-9]+,[0-9]+\\))";
        Pattern pattern2 = Pattern.compile(regex2);
        Matcher m2 = pattern2.matcher("double(2,8)");
        if (m2.find()){
            System.out.println(m2.group(1));
        }
    }

}
