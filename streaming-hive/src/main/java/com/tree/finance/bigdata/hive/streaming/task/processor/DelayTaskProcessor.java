package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.imutable.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.imutable.ConfigHolder;
import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.exeption.DistributeLockException;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.consumer.mysql.MysqlTask;
import com.tree.finance.bigdata.hive.streaming.task.listener.DbTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.utils.CombinedMutation;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/9/19 15:20
 */
public class DelayTaskProcessor {

    private AppConfig config = ConfigHolder.getConfig();

    private static Logger LOG = LoggerFactory.getLogger(DelayTaskProcessor.class);

    private ConnectionFactory factory = ConfigHolder.getDbFactory();

    private DbTaskStatusListener dbTaskStatusListener = new DbTaskStatusListener(factory);

    private LinkedBlockingQueue<MysqlTask> taskQueue = new LinkedBlockingQueue<>(config.getDelayTaskQueueSize());

    private Thread thread;

    private volatile boolean stop = false;

    private int id;

    public DelayTaskProcessor(int id) {
        this.id = id;
        this.thread = new Thread(this::run, "DelayTaskProcessor-" + id);
    }

    public void init() {
        this.thread.start();
    }

    public void run() {
        while (!stop || !taskQueue.isEmpty()) {
            try {
                MysqlTask task = taskQueue.poll(10, TimeUnit.SECONDS);
                if (null == task) {
                    continue;
                }
                handleDelayTask(task);
            } catch (InterruptedException e) {
                // no opt
            } catch (Throwable t) {
                LOG.error("unexpected ERROR !!!", t);
            }
        }
        LOG.info("{}, stopped", Thread.currentThread().getName());
    }

    public void handleDelayTask(MysqlTask task) {
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            Path path = new Path(task.getTaskInfo().getFilePath());
            if (!fileSystem.exists(path)) {
                LOG.info("path not exist: {}", path);
                dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
                return;
            }
        } catch (Exception e) {
            LOG.error("maybe hadoop file system unavailable, skip task: {}", task.getTaskInfo().getFilePath(), e);
        }

        if (task.getTaskInfo().getOp().equals(Operation.ALL)) {
            handleCombineTask(task);
        } else {
            LOG.info("found task type not combine task type, {}", task.getTaskInfo());
            dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
        }

    }

    private void handleCombineTask(MysqlTask task) {
        CombinedMutation mutationUtils = new CombinedMutation(task.getTaskInfo().getDb(),
                task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(),
                task.getTaskInfo().getPartitions(), config.getMetastoreUris(), ConfigFactory.getHbaseConf());
        try {
            long start = System.currentTimeMillis();
            LOG.info("handle delayed combine task: {}", task.getTaskInfo().getFilePath());
            try (AvroFileReader reader = new AvroFileReader(new Path(task.getTaskInfo().getFilePath()))) {
                Schema recordSchema = reader.getSchema();
                mutationUtils.beginTransaction(recordSchema, ConfigHolder.getHiveConf());
                while (reader.hasNext()) {
                    GenericData.Record record = reader.next();
                    mutationUtils.mutate(record);
                }
            }
            mutationUtils.commitTransaction();
            dbTaskStatusListener.onTaskSuccess(task.getTaskInfo());
            LOG.info("delay combine task success : {}, cost: {}ms", task.getTaskInfo().getFilePath(),
                    System.currentTimeMillis() - start);
        } catch (DistributeLockException e) {
            LOG.warn("failed get zk lock for delay task: {}", task.getTaskInfo().getFilePath());
            mutationUtils.abortTxn();
        } catch (TransactionException e) {
            try {
                if (e.getCause() instanceof LockException) {
                    LOG.warn("failed to lock for delay combine task, txnId: {}, file: {}", mutationUtils.getTransactionId(),
                            task.getTaskInfo().getFilePath());
                } else {
                    LOG.error("txn exception", e);
                }
                mutationUtils.abortTxn();
            } catch (Exception ex) {
                LOG.error("error handle transaction exception", ex);
            }
        } catch (Throwable t) {
            LOG.error("file task failed: {}", task.getTaskInfo(), t);
            try {
                mutationUtils.abortTxn();
                dbTaskStatusListener.onTaskError(task.getTaskInfo());
            } catch (Throwable e) {
                LOG.error("error abort txn", e);
            }
        }
    }

    public void commit(MysqlTask consumedTask) throws InterruptedException {
        this.taskQueue.put(consumedTask);
    }

    public void stop() {
        this.stop = true;
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping DelayTaskProcessor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped DelayProcessor-{}", id);
    }

}
