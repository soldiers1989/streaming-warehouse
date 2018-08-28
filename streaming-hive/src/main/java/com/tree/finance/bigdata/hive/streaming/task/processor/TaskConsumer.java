package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.task.consumer.RabbitMqTaskConsumer;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/25 11:06
 */
public class TaskConsumer implements Service{

    private Thread consumeThread;

    private volatile boolean running = true;

    private com.tree.finance.bigdata.hive.streaming.task.consumer.TaskConsumer taskConsumer;

    private static Logger LOG = LoggerFactory.getLogger(TaskConsumer.class);

    private TaskDispatcher processor;

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public TaskConsumer(AppConfig config, TaskDispatcher processor) {
        this.taskConsumer = new RabbitMqTaskConsumer(config);
        this.processor = processor;
        consumeThread = new Thread(this::fetchTask, "TaskDispatcher");
    }

    @Override
    public void init() {
        consumeThread.start();
        LOG.info("started Consumer");
    }

    private void fetchTask() {
        LOG.info("started Fetch Thread");
        while (running) {
            try {
                ConsumedTask consumedTask = taskConsumer.consume();
                processor.dispatch(consumedTask);
            } catch (Throwable e) {
                LOG.error("", e);
            }
        }
        countDownLatch.countDown();
        LOG.info("terminated TaskDispatcher");
    }

    public void stop() throws InterruptedException{
        this.running = false;
        this.consumeThread.interrupt();
        countDownLatch.await();
    }
}
