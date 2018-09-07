package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveDDLUtils;

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
            hiveDDLUtils.createTable(parser.getDb(), parser.getTable(), parser.includeDigits());
        }
    }

}
