package com.tree.finance.bigdata.hive.streaming.tools.recId.insight;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.utils.HbaseUtils;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class InsightTools {

    static final String HBASE_TBL_SUFFIX = "_id";

    static final byte[] RECID_COL = Bytes.toBytes("recordId");

    static final byte[] UPDATE_TIME_COL = Bytes.toBytes("update_time");

    static final byte[] FAMLIY = Bytes.toBytes("f");

    public static void main(String[] args) throws Exception{
        InsightParser parser = new InsightParser(args);
        try {
            parser.init();
        }catch (ParseException e) {
            parser.printHelp();
            System.exit(1);
        }
        if (parser.isHelp()) {
            parser.printHelp();
        }
        String[]  bizIds = parser.getIds();
        String table = parser.getTable();
        if (!table.endsWith(HBASE_TBL_SUFFIX)) {
            table = table + HBASE_TBL_SUFFIX;
        }
        getAndPrint(table, bizIds);
    }

    private static void getAndPrint(String table, String[] bizIds) throws Exception{
        HbaseUtils hbaseUtils = HbaseUtils.getTableInstance(table, ConfigFactory.getHbaseConf());
        List<Get> gets = new ArrayList<>();
        for (String id : bizIds) {
            gets.add(new Get(Bytes.toBytes(GenericRowIdUtils.addIdWithHash(id))));
        }
        Result[]  results = hbaseUtils.getAll(gets);
        if (results.length !=  gets.size()) {
            throw new RuntimeException("error");
        }

        String tempLate = "%25s %16s %s";
        System.out.println(String.format(tempLate, "id", "recordId", "update_time"));

        for (int i =0; i<results.length; i++) {
            if (results[i].isEmpty()) {
                System.out.println(String.format(tempLate, bizIds[i], "EMPTY", "EMPTY"));
            } else {
                String recId = Bytes.toString(results[i].getValue(FAMLIY, RECID_COL));
                String updateTime = Long.toString(Bytes.toLong(results[i].getValue(FAMLIY, UPDATE_TIME_COL)));
                System.out.println(String.format(tempLate, bizIds[i], recId, updateTime));
            }
        }

        hbaseUtils.close();

    }
}
