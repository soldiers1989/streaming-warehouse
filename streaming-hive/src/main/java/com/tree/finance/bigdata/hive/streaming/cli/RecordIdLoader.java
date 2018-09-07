package com.tree.finance.bigdata.hive.streaming.cli;

import com.tree.finance.bigdata.hive.streaming.utils.record.RecordIdLoaderTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/6 11:23
 */
public class RecordIdLoader {

    static Logger LOG = LoggerFactory.getLogger(RecordIdLoader.class);

    public static void main(String[] args) throws Exception{

        RecordIdLoaderParser parser = null;
        try {
            parser = new RecordIdLoaderParser(args);
            parser.init();
        }catch (Exception e){
            LOG.error("", e);
            parser.printHelp();
        }

        new RecordIdLoaderTools(parser.getDb(), parser.getTable(), parser.getCores()).load();

    }
}
