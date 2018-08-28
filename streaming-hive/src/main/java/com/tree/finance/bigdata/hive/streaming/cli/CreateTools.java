package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveAdminUtils;
import com.tree.finance.bigdata.hive.streaming.utils.hive.HiveDDLUtils;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 15:40
 */
public class CreateTools {

    public static void main(String[] args) throws Exception{

        AppConfig config = ConfigHolder.getConfig();

        CreateTableParser parser = null;
        try {
            parser = new CreateTableParser(args);
            parser.init();
        }catch (Exception e){
            e.printStackTrace();
            parser.printHelp();
        }


        HiveDDLUtils hiveDDLUtils = new HiveDDLUtils(config.getSourceDblUrl(), config.getSourceDbUser(),
                config.getSourceDbPwd(), config.getDefaultClusterCols());

        if (parser.isHelp()){
            parser.printHelp();
            return;
        }

        List<String> ddls = hiveDDLUtils.createDDLFromMysql(parser.getDb(), parser.getTable(), parser.includeDigits());

        new HiveAdminUtils(config.getHiveServer2Url()).createTable(ddls);
    }

}
