package com.tree.finance.bigdata.hive.streaming.utils.hive;

import com.tree.finance.bigdata.hive.streaming.config.Constants;
import com.tree.finance.bigdata.hive.streaming.utils.common.StringUtils;
import com.tree.finance.bigdata.hive.streaming.utils.record.RecordUtils;
import org.apache.hive.jdbc.HiveDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 12:36
 */
public class HiveDDLUtils {

    private String jdbcUrl;
    private String user;
    private String password;
    private List<String> globalClusterCols;
    private static final Logger LOG = LoggerFactory.getLogger(HiveDDLUtils.class);

    public HiveDDLUtils(String jdbcUrl, String user, String password, String globaClusterCols) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.globalClusterCols = Arrays.asList(globaClusterCols.split(","));
    }

    public List<String> createDDLFromMysql(String db, String[] tbls, boolean includeDigit) throws Exception {

        List<String> ddls = new ArrayList<>();
        ddls.add("create schema if not exists " + db);
        String createTemplate = getCreateTblTemplate();
        Class.forName(HiveDriver.class.getName());
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();

            if (null == tbls || tbls.length == 0) {
                ResultSet tblSet = databaseMetaData.getTables(db, db, "%", null);
                List<String> tableList = new ArrayList<>();
                while (tblSet.next()) {
                    String tableName = tblSet.getString("TABLE_NAME");
                    tableList.add(tableName);
                }
                tbls = tableList.toArray(new String[tableList.size()]);
            }
            for (String tbl : tbls) {
                Map<String, String> colMaps = new TreeMap<>();
                //get column name to type maps
                String stmt = "describe " + db + "." + tbl;
                ResultSet resultSet = statement.executeQuery(stmt);

                while (resultSet.next()) {
                    String column = resultSet.getString("Field").toLowerCase();
                    String type = resultSet.getString("Type");
                    type = HiveTypeConvertor.toHiveType(type);
                    colMaps.put(column, type);
                }

                String dbName = db;
                if (! includeDigit) {
                    String regex = "([a-zA-Z\\-_]+)([0-9]+)";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher m = pattern.matcher(db);
                    if (m.find()) {
                        dbName = m.group(1);
                    }
                }

                String ddl = createTemplate.replace("${table_name}", dbName + "." + tbl);
                String columnStr = buildColumn(colMaps);
                String clusterColStr = buildClusterColumn(tbl, colMaps);

                if (StringUtils.isEmpty(clusterColStr)) {
                    System.out.println("no partition columns found for table: " + tbl);
                    continue;
                }

                ddl = ddl.replace("${tbl_columns}", columnStr);
                ddl = ddl.replace("${cluster_columns}", clusterColStr);

                LOG.info("ddl for table: {} is: [{}]", tbl, ddl);
                ddls.add(ddl);
            }
        }
        return ddls;
    }

    private String buildClusterColumn(String tbl, Map<String, String> map) {
        return RecordUtils.getCreateTimeCol(tbl, map.keySet());
    }

    private String buildColumn(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("(");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(' ').append(entry.getValue()).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(')');
        return sb.toString();
    }

    private String getCreateTblTemplate() throws Exception {
        StringBuilder sb = new StringBuilder();
        try (InputStream inputStream = HiveDDLUtils.class.getResourceAsStream(Constants.HIVE_CREATE_TBL_TEMPLATE_FILE);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while (null != (line = reader.readLine())) {
                sb.append(line).append(' ');
            }
        }
        return sb.toString();
    }

}
