package com.tree.finance.bigdata.kafka.connect.sink.fs.partition;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 10:40
 */
public class TestPartitionHelper {

    @Test
    public void testPartition() throws Exception{
        URI uri =TestPartitionHelper.class.getResource("/t_cash_pos_new_history.avro").toURI();

//        Schema afterSchema = SchemaBuilder.struct().field("lastupdatedate",)

//        Struct struct = new Struct()
        try (
                DataFileReader reader = new DataFileReader(new File(uri.getPath()), new GenericDatumReader());
        ) {
            while (reader.hasNext()) {
                System.out.println(reader.next());
            }
        }
    }


    @Test
    public void testInsert() {


        long start2 = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            DateTime dateTime = new DateTime();
            dateTime.toString("yyyy-MM-dd HH:mm:ss");
        }
        System.out.println(System.currentTimeMillis() - start2);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            Date date = new Date();
            sdf.format(date);
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testPartitionHelper() {
        new PartitionHelper().getYmdFromDateStr("2018-07-11T10:00:00+08:00", '-');
    }
}
