package com.tree.finance.bigdata.hive.streaming.task;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 20:51
 */
public class RabiitMqTaskConsumerTest {

    String testQueueName = "streaming_warehouse_file_queue";

    @Test
    public void testConsume() {
        try {

            ConnectionFactory factory = new ConnectionFactory();
            //10.1.2.207 5672
            factory.setHost("127.0.0.1");
            factory.setPort(5672);
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(testQueueName, false, consumer);
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String msg = new String(delivery.getBody());
            System.out.println("msg: " + msg);
            // do something with msg.
            System.out.println("consumerTag: " + consumer.getConsumerTag());
            channel.basicCancel(consumer.getConsumerTag());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testProduce() throws Exception{
        //创建连接和频道
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明队列
        channel.queueDeclare(testQueueName, true, false, false, null);
        //发送10条消息，依次在消息后面附加1-10个点
        channel.basicPublish("", testQueueName, null, "msg 2".getBytes());
        //关闭频道和资源
        channel.close();
        connection.close();

    }
}
