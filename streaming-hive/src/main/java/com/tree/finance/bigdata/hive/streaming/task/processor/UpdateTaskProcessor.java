package com.tree.finance.bigdata.hive.streaming.task.processor;

import com.tree.finance.bigdata.hive.streaming.config.AppConfig;
import com.tree.finance.bigdata.hive.streaming.config.Constants;
import com.tree.finance.bigdata.hive.streaming.exeption.DataDelayedException;
import com.tree.finance.bigdata.hive.streaming.mutation.AvroMutationFactory;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import com.tree.finance.bigdata.hive.streaming.reader.AvroFileReader;
import com.tree.finance.bigdata.hive.streaming.task.listener.HiveLockFailureListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.MqTaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.listener.TaskStatusListener;
import com.tree.finance.bigdata.hive.streaming.task.type.ConsumedTask;
import com.tree.finance.bigdata.hive.streaming.utils.common.StringUtils;
import com.tree.finance.bigdata.hive.streaming.utils.hbase.HbaseUtils;
import com.tree.finance.bigdata.hive.streaming.utils.metric.MetricReporter;
import com.tree.finance.bigdata.task.Operation;
import io.prometheus.client.Summary;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hive.hcatalog.streaming.mutate.client.AcidTable;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClientBuilder;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinatorBuilder;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.tree.finance.bigdata.hive.streaming.config.Constants.AVRO_KEY_RECORD_ID;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/17 10:28
 */
public class UpdateTaskProcessor implements Runnable {

    private AppConfig config;

    private static TaskStatusListener taskStatusListener;

    private static Logger LOG = LoggerFactory.getLogger(InsertTaskProcessor.class);

    private LinkedBlockingQueue<ConsumedTask> taskQueue;

    private volatile boolean stop = false;

    private Thread thread;

    private int id;

    UpdateTaskProcessor(AppConfig config, int id) {
        this.config = config;
        this.id = id;
        this.taskQueue = new LinkedBlockingQueue<>(config.getFileQueueSize());
        this.taskStatusListener = new MqTaskStatusListener(config);
        this.thread = new Thread(this::run, "Update-Processor-" + id);
    }

    public void init() {
        this.thread.start();
    }

    @Override
    public void run() {

        while (!stop || !taskQueue.isEmpty()) {
            try {
                ConsumedTask task = null;
                try {
                    task = taskQueue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (task == null) {
                    continue;
                }
                handle(task);
            } catch (Throwable t) {
                LOG.error("unexpected error", t);
            }
        }

        LOG.info("Update-Processor-{} stopped", id);

    }

    private void handle(ConsumedTask task) {
        MutatorClient mutatorClient = null;
        Transaction mutateTransaction = null;
        HbaseUtils hbaseUtil = null;
        MutatorCoordinator mutateCoordinator = null;

        Summary.Timer timer = MetricReporter.startUpdate();

        long startTime = System.currentTimeMillis();
        LOG.info("file task start: {}", task.getTaskInfo().getFilePath());

        try {
            Path path = new Path(task.getTaskInfo().getFilePath());
            FileSystem fileSystem = FileSystem.get(new Configuration());

            if (!fileSystem.exists(path)) {
                LOG.warn("path not exist: {}", task.getTaskInfo().getFilePath());
                taskStatusListener.onTaskSuccess(task);
                return;
            }

            Long bytes = fileSystem.getFileStatus(path).getLen();

            AvroFileReader reader = new AvroFileReader(path);
            Schema recordSchema = reader.getSchema();

            hbaseUtil = HbaseUtils.getTableInstance(config.getRowIdToRecIdHbaseTbl());
            byte[] columnFamily = Bytes.toBytes(config.getDefaultColFamily());
            byte[] colQualifier = Bytes.toBytes(config.getRecordIdQualifier());

            TreeMap<String, GenericData.Record> recordIdsortedRecord = new TreeMap<>(Comparator.reverseOrder());

            String dbTblPrefix = task.getTaskInfo().getDb() + "." + task.getTaskInfo().getTbl() + '_';
            //recordId, 降序排列
            while (reader.hasNext()) {
                GenericData.Record record = reader.next();
                GenericData.Record keyRecord = (GenericData.Record) record.get(Constants.AVRO_KEY_RECORD_ID);
                String businessId = GenericRowIdUtils.assembleBuizId(keyRecord, recordSchema.getField(AVRO_KEY_RECORD_ID).schema());
                //recordId= transactionId_BUCKET_ID _rowId
                String recordId = hbaseUtil.getValue(dbTblPrefix + businessId, columnFamily, colQualifier);
                if (StringUtils.isEmpty(recordId)) {
                    LOG.warn("no recordId found for: {}, data maybe delayed", dbTblPrefix + businessId);
                    throw new DataDelayedException("no recordId found for " + dbTblPrefix + businessId);
                }
                recordIdsortedRecord.put(recordId, record);
            }

            MutatorFactory factory = new AvroMutationFactory(new Configuration(), new AvroObjectInspector(
                    task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(), recordSchema));
            mutatorClient = new MutatorClientBuilder()
                    .addSinkTable(task.getTaskInfo().getDb(), task.getTaskInfo().getTbl(), task.getTaskInfo().getPartitionName(), true)
                    .metaStoreUri(config.getMetastoreUris())
                    .lockFailureListener(new HiveLockFailureListener())
                    .build();
            mutatorClient.connect();

            List<AcidTable> destinations = mutatorClient.getTables();
            mutateTransaction = mutatorClient.newTransaction();


            mutateTransaction.begin();
            mutateCoordinator = new MutatorCoordinatorBuilder()
                    .metaStoreUri(config.getMetastoreUris())
                    .table(destinations.get(0))
                    .mutatorFactory(factory)
                    .build();

            List<String> partitions = task.getTaskInfo().getPartitions();

            if (null == partitions) {
                LOG.error("found no partition values from task info: {}", task.getTaskInfo());
                throw new RuntimeException("found no partition values");
            }

            for (GenericData.Record record : recordIdsortedRecord.values()) {

                if (record == null) {
                    continue;
                }

                if (Operation.DELETE.code().equals(record.get("op").toString())) {
                    mutateCoordinator.delete(partitions, record);
                } else {
                    mutateCoordinator.update(partitions, record);
                }
            }

            mutateCoordinator.close();
            mutateTransaction.commit();
            taskStatusListener.onTaskSuccess(task);

            MetricReporter.updatedBytes(bytes);

            long endTime = System.currentTimeMillis();
            LOG.info("file task success: {} cost: {}ms", task.getTaskInfo().getFilePath(), endTime - startTime);

        } catch (DataDelayedException e) {
            taskStatusListener.onTaskDelay(task);
        } catch (Throwable t) {
            LOG.error("file task failed: " + task.getTaskInfo().getFilePath(), t);
            try {
                taskStatusListener.onTaskError(task);
                closeQuietly(mutateCoordinator);
                if (mutateTransaction != null) {
                    mutateTransaction.abort();
                }
            } catch (Exception e) {
                LOG.error("error abort txn", e);
            }
        } finally {
            try {
                timer.observeDuration();
                if (null != mutatorClient) {
                    mutatorClient.close();
                }
                if (null != hbaseUtil) {
                    hbaseUtil.close();
                }
            } catch (Exception e) {
                LOG.error("error closing client", e);
            }
        }
    }

    private void closeQuietly(MutatorCoordinator mutatorCoordinator) {
        try {
            if (null != mutatorCoordinator) {
                mutatorCoordinator.close();
            }
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    public void process(ConsumedTask consumedTask) {
        this.taskQueue.offer(consumedTask);
    }

    public void stop() {

        this.stop = true;
        while (taskQueue.size() != 0) {
            try {
                LOG.info("stopping Update-Processor-{}, remaining {} tasks...", id, taskQueue.size());
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                //no pots
            }
        }
        LOG.info("stopped Update-Processor-{}", id);
    }

}
