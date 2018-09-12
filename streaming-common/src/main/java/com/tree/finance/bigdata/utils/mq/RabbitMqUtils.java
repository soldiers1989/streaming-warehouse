package com.tree.finance.bigdata.utils.mq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 17:00
 */
public class RabbitMqUtils {

    private String host;
    private int port;
    private ConnectionFactory factory;
    private Connection connection;
    private Logger LOG = LoggerFactory.getLogger(RabbitMqUtils.class);
    private static HashMap<String, RabbitMqUtils> map = new HashMap<>();
    private HashMap<String, QueueingConsumer> queueMap = new HashMap<>();

    public static synchronized RabbitMqUtils getInstance(String host, int port) {
        String key = host + "_" + port;
        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            RabbitMqUtils rabbitMqUtils = new RabbitMqUtils(host, port);
            rabbitMqUtils.init();
            map.put(key, rabbitMqUtils);
            return rabbitMqUtils;
        }
    }

    private RabbitMqUtils(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private void init() {
        try {
            this.factory = new ConnectionFactory();
            factory.setHost(host);
            factory.setConnectionTimeout(10000);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setPort(port);
            connection = factory.newConnection();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param queue    队列名称
     * @param messages 要发送的消息
     * @return 发送失败的消息
     * @throws Exception
     */
    public List<String> produce(String queue, List<String> messages) throws Exception {
        Channel channel = null;
        List<String> sent = new ArrayList<>();
        String sending = "";
        try {
            channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(queue, true, false, false, null);
            for (String msg : messages) {
                sending = msg;
                channel.basicPublish("", queue, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());
                sent.add(msg);
            }
        } catch (Exception e) {
            LOG.error("send failed: {}", sending);
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
        messages.removeAll(sent);
        return messages;
    }

    public void produce(String queue, String message) throws Exception {
        Channel channel = null;
        try {
            channel = connection.createChannel();
            //声明队列
            channel.queueDeclare(queue, true, false, false, null);
            channel.basicPublish("", queue, null, message.getBytes());
        } finally {
            if (channel != null) {
                channel.close();
            }
        }
    }

    /**
     * @param queue
     * @return 消费的消息和当前channel
     */
    public ChannelMsg consume(String queue, int Qos) throws IOException, InterruptedException {
        if (!queueMap.containsKey(queue)) {
            synchronized (queueMap) {
                if (!queueMap.containsKey(queue)) {
                    Channel channel = connection.createChannel();
                    channel.basicQos(Qos, true);
                    QueueingConsumer consumer = new QueueingConsumer(channel);
                    channel.basicConsume(queue, false, consumer);
                    queueMap.put(queue, consumer);
                }
            }
        }
        QueueingConsumer consumer = queueMap.get(queue);
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        return new ChannelMsg(consumer.getChannel(), delivery, consumer);
    }

    public void close() {
        try {
            this.connection.close();
        } catch (Exception e) {
            LOG.error("error closing", e);
        }
    }

}
