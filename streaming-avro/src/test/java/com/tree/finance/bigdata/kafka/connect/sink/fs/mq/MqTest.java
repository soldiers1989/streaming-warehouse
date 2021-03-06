package com.tree.finance.bigdata.kafka.connect.sink.fs.mq;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.utils.mq.ChannelMsg;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/16 11:23
 */
public class MqTest {

    java.lang.String queueName = "streaming_warehouse_file_queue";

    @Test
    public void sentMqTask() throws Exception {
        String path = "/data/kafka-connect/sink/UPDATE/test/par_test/y=2018/m=07/d=11/1/0-192.168.201.138-1531807836556.done";
        TaskInfo taskInfo = new TaskInfo("test-id", "test", "par_test", Lists.newArrayList("2018", "07", "11")
                , "y=2018/m=07/d=11", path, Operation.UPDATE);
        String msgBody = JSON.toJSONString(taskInfo);
        RabbitMqUtils rabbitMqUtils = RabbitMqUtils.getInstance("localhost", 5672);
        rabbitMqUtils.produce(queueName, msgBody);
    }

    @Test
    public void consumeMqTask() throws Exception {
        RabbitMqUtils rabbitMqUtils = RabbitMqUtils.getInstance("10.1.2.207", 5672);
        while (true) {
            ChannelMsg channelMsg = rabbitMqUtils.consume(queueName, 10);
            System.out.println(new String(channelMsg.getMsg().getBody()));
            channelMsg.getChannel().basicAck(channelMsg.getMsg().getEnvelope().getDeliveryTag(), false);
        }
//        ChannelMsg channelMsg2 =rabbitMqUtils.consume(queueName);
//        System.out.println(new String(channelMsg2.getMsg().getBody()));
//        System.out.println("#######");
    }
}
