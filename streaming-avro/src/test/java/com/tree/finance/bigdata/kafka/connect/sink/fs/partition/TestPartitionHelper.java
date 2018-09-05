package com.tree.finance.bigdata.kafka.connect.sink.fs.partition;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 10:40
 */
public class TestPartitionHelper {
    @Test
    public void testInsert(){


        long start2 = System.currentTimeMillis();
        for (int i=0; i < 100000000; i++){
            DateTime dateTime = new DateTime();
            dateTime.toString("yyyy-MM-dd HH:mm:ss");
        }
        System.out.println(System.currentTimeMillis() -  start2);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        for (int i=0; i < 100000000; i++){
            Date date = new Date();
            sdf.format(date);
        }
        System.out.println(System.currentTimeMillis() -  start);
    }
    @Test
    public void testPartitionHelper(){
        new PartitionHelper(null).getYmdFromDateStr("2018-07-11T10:00:00+08:00", '-');
    }
}
