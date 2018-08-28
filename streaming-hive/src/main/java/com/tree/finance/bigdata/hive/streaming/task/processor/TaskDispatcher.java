package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.task.Operation;
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

    public TaskDispatcher(AppConfig config) {
        this.config = config;
    }

    public void init() {
        this.insertExecutor = new InsertTaskProcessor[config.getInsertProcessorCores()];
        for (int i = 0; i < config.getInsertProcessorCores(); i++) {
            insertExecutor[i] = new InsertTaskProcessor(config, i);
            insertExecutor[i].init();
        }

        this.updateExecutor = new UpdateTaskProcessor[config.getUpdateProcessorCores()];
        for (int i = 0; i < config.getUpdateProcessorCores(); i++) {
            updateExecutor[i] = new UpdateTaskProcessor(config, i);
            updateExecutor[i].init();
        }

        LOG.info("started TaskProcessor ...");
    }

    public void dispatch(ConsumedTask consumedTask) {

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

    @Override
    public void stop() {

        for (InsertTaskProcessor insertTaskProcessor : insertExecutor) {
            try {
                insertTaskProcessor.stop();
            } catch (Exception e) {
                LOG.error("error stopping insert ");
            }
            LOG.info("stopped TaskProcessor");
        }

        for (UpdateTaskProcessor updateTaskProcessor : updateExecutor) {
            try {
                updateTaskProcessor.stop();
            } catch (Exception e) {
                LOG.error("error stopping insert ");
            }
            LOG.info("stopped TaskProcessor");
        }

    }
}
