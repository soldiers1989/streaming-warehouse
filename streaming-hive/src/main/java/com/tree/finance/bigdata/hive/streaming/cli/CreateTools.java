package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveAdminUtils;
import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveDDLUtils;

import java.util.List;
import java.util.Properties;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.MYSQL_DB_CONF_FILE;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 15:40
 */
public class CreateTools {

    public static void main(String[] args) throws Exception{

        CreateTableParser parser = null;
        try {
            parser = new CreateTableParser(args);
            parser.init();
        }catch (Exception e){
            e.printStackTrace();
            parser.printHelp();
        }
        if (parser.isHelp()){
            parser.printHelp();
            return;
        }

        Properties properties = new Properties();
        properties.load(CreateTools.class.getResourceAsStream(MYSQL_DB_CONF_FILE));
        HiveDDLUtils hiveDDLUtils = new HiveDDLUtils(properties);


        if (!parser.dbSpecified()) {
            hiveDDLUtils.createAllTables();
        } else {
            hiveDDLUtils.createDDLFromMysql(parser.getDb(), parser.getTable(), parser.includeDigits());
        }
    }

}
