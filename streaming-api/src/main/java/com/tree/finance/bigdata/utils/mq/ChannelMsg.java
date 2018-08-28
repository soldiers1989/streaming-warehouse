package com.tree.finance.bigdata.utils.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/5 17:21
 */
public class ChannelMsg {

    private Channel channel;
    private QueueingConsumer.Delivery msg;
    private QueueingConsumer consumer;
    public ChannelMsg(Channel channel, QueueingConsumer.Delivery msg, QueueingConsumer consumer) {
        this.channel = channel;
        this.consumer = consumer;
        this.msg = msg;
    }

    public QueueingConsumer getConsumer(){
        return consumer;
    }

    public Channel getChannel() {
        return channel;
    }

    public QueueingConsumer.Delivery getMsg() {
        return msg;
    }
}
