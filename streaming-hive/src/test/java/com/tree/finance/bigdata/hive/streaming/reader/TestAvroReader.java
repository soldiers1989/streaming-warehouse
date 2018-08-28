package com.tree.finance.bigdata.hive.streaming.reader;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 15:00
 */
public class TestAvroReader {

    @Test
    public void testAvroReader() {

        Path path = new Path("/data/kafka-connect/sink/UPDATE/test/par_test/y=2018/m=07/d=11/1/0-192.168.201.138-1531740649131.done.sent");
        try (AvroFileReader avroFileReader = new AvroFileReader(path)) {
            System.out.println(avroFileReader.getSchema());
            while (avroFileReader.hasNext()) {
                System.out.println(avroFileReader.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
