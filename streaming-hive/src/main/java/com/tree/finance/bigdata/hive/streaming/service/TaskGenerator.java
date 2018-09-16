package com.tree.finance.bigdata.hive.streaming.service;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTaskConsumer;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTaskConsumer;
import com.tree.finance.bigdata.utils.common.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/25 11:06
 */
public class TaskGenerator implements Service {

    private Thread consumeThread;

    private Thread delayScheduler;

    private volatile boolean running = true;

    private RabbitMqTaskConsumer mqTaskConsumer;
    private MysqlTaskConsumer mysqlTaskConsumer;

    private static Logger LOG = LoggerFactory.getLogger(TaskGenerator.class);

    private TaskDispatcher dispatcher;

    private CountDownLatch countDownLatch = new CountDownLatch(2);

    public TaskGenerator(AppConfig config, TaskDispatcher dispatcher) {
        this.mqTaskConsumer = new RabbitMqTaskConsumer(config);
        this.mysqlTaskConsumer = new MysqlTaskConsumer();
        this.dispatcher = dispatcher;
        consumeThread = new Thread(this::fetchMqTask, "TaskDispatcher");
        delayScheduler = new Thread(this::fetchDelayTask, "DelayTaskFetcher");
    }

    private void fetchDelayTask() {
        LOG.info("started DelayTaskFetcher");
        while (running) {
            try {
                Thread.sleep(ConfigHolder.getConfig().getDelayScheduleMin() * 60 * 1000);
                List<MysqlTask> consumedTask = mysqlTaskConsumer.consumeBatch();
                if (CollectionUtils.isEmpty(consumedTask)) {
                    LOG.info("no delayed tasks");
                    continue;
                }
                LOG.info("going to dispatch {} delayed task", consumedTask.size());
                for (MysqlTask mysqlTask : consumedTask) {
                    dispatcher.dispatch(mysqlTask);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                LOG.error("", e);
            }
        }
        countDownLatch.countDown();
        LOG.info("terminated TaskDispatcher");
    }

    @Override
    public void init() {
        consumeThread.start();
        delayScheduler.start();
        LOG.info("started Consumer");
    }

    private void fetchMqTask() {
        LOG.info("started Fetch Thread");
        while (running) {
            try {
                RabbitMqTask consumedTask = mqTaskConsumer.consume();
                dispatcher.dispatch(consumedTask);
            } catch (Throwable e) {
                LOG.error("", e);
            }
        }
        countDownLatch.countDown();
        LOG.info("terminated TaskDispatcher");
    }

    public void stop() throws InterruptedException {
        this.running = false;
        this.consumeThread.interrupt();
        this.delayScheduler.interrupt();
        countDownLatch.await();
    }
}
