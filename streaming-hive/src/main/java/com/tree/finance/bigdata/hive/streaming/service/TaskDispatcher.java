package com.tree.finance.bigdata.hive.streaming.service;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.task.consumer.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mq.RabbitMqTask;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
import com.tree.finance.bigdata.hive.streaming.task.processor.InsertTaskProcessor;
import com.tree.finance.bigdata.hive.streaming.task.processor.UpdateTaskProcessor;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:09
 */
public class TaskDispatcher implements Service {

    private static Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);

    private InsertTaskProcessor[] insertExecutor;

    private UpdateTaskProcessor[] updateExecutor;

    private AppConfig config;

    private ConnectionFactory factory;

    public TaskDispatcher(AppConfig config) {
        this.config = config;
    }

    public void init() {
        this.factory = ConfigHolder.getDbFactory();

        this.insertExecutor = new InsertTaskProcessor[config.getInsertProcessorCores()];
        for (int i = 0; i < config.getInsertProcessorCores(); i++) {
            insertExecutor[i] = new InsertTaskProcessor(config, factory, i);
            insertExecutor[i].init();
        }

        this.updateExecutor = new UpdateTaskProcessor[config.getUpdateProcessorCores()];
        for (int i = 0; i < config.getUpdateProcessorCores(); i++) {
            updateExecutor[i] = new UpdateTaskProcessor(config, factory, i);
            updateExecutor[i].init();
        }

        LOG.info("started TaskProcessor ...");
    }

    public void dispatch(RabbitMqTask consumedTask) {
        if (null == consumedTask) {
            return;
        }
        int hash = Math.abs(Objects.hash(consumedTask.getTaskInfo().getDb(), consumedTask.getTaskInfo().getTbl(),
                consumedTask.getTaskInfo().getPartitionName()));

        if (Operation.CREATE.equals(consumedTask.getTaskInfo().getOp())) {
            insertExecutor[hash % insertExecutor.length].process(consumedTask);
        } else if (Operation.UPDATE.equals(consumedTask.getTaskInfo().getOp()) || Operation.DELETE.equals(consumedTask.getTaskInfo().getOp())) {
            updateExecutor[hash % updateExecutor.length].process(consumedTask);
        } else {
            LOG.error("unsupported task type: {}", consumedTask.getTaskInfo());
            consumedTask.taskRejected();
        }
    }

    public void dispatch(MysqlTask consumedTask) {
        if (null == consumedTask) {
            return;
        }
        //handle by scheduler thread
        if (Operation.CREATE.equals(consumedTask.getTaskInfo().getOp())) {
            insertExecutor[0].handleMysqlTask(consumedTask);
        } else if (Operation.UPDATE.equals(consumedTask.getTaskInfo().getOp()) || Operation.DELETE.equals(consumedTask.getTaskInfo().getOp())) {
            updateExecutor[0].handleMysqlTask(consumedTask);
        } else {
            LOG.error("unsupported task type: {}", consumedTask.getTaskInfo());
            consumedTask.taskRejected();
        }
    }

    @Override
    public void stop() {

        for (InsertTaskProcessor insertTaskProcessor : insertExecutor) {
            try {
                insertTaskProcessor.stop();
            } catch (Exception e) {
                LOG.error("error stopping insert ");
            }
            LOG.info("stopped insert TaskProcessor");
        }

        for (UpdateTaskProcessor updateTaskProcessor : updateExecutor) {
            try {
                updateTaskProcessor.stop();
            } catch (Exception e) {
                LOG.error("error stopping insert ");
            }
            LOG.info("stopped update TaskProcessor");
        }

        this.factory.close();

    }
}
