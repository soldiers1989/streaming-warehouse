package com.tree.finance.bigdata.hive.streaming.tools.cli;


import com.tree.finance.bigdata.hive.streaming.tools.config.Constants;
import com.tree.finance.bigdata.hive.streaming.tools.hive.HiveDDLUtils;

import java.util.Properties;


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
        properties.load(CreateTools.class.getResourceAsStream(Constants.MYSQL_DB_CONF_FILE));
        HiveDDLUtils hiveDDLUtils = new HiveDDLUtils(properties);


        if (!parser.dbSpecified()) {
            hiveDDLUtils.createAllTables();
        } else {
            hiveDDLUtils.createTable(parser.getDb(), parser.getTable(), parser.includeDigits());
        }
    }

}
