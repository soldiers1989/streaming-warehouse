package com.tree.finance.bigdata.hive.streaming.utils.hive;

import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 16:07
 */
public class HiveAdminUtils {

    private String hive2Url;

    public HiveAdminUtils(String hive2Url) {
        this.hive2Url = hive2Url;
    }


    public void createTable(List<String> ddls) throws Exception {
        try (Connection connection = DriverManager.getConnection(hive2Url);
             Statement statement = connection.createStatement()) {
            for (String ddl : ddls) {
                try {
                    statement.execute(ddl);
                }catch (Exception e) {
                    System.out.println("failed to execute: " + ddl);
                    e.printStackTrace();
                }

            }
        }
    }

}
