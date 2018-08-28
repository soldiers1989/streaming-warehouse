package com.tree.finance.bigdata.kafka.connect.sink.fs.partition;

import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 10:40
 */
public class TestPartitionHelper {
    @Test
    public void testPartitionHelper(){
        new PartitionHelper(null).getYmdFromDateStr("2018-07-11T10:00:00+08:00", '-');
    }
}
