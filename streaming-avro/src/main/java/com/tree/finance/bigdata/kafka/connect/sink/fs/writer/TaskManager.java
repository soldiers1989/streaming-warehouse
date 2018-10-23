package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.PioneerConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.FileSuffix;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.task.TaskInfo;
import com.tree.finance.bigdata.task.TaskStatus;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import com.tree.finance.bigdata.utils.mysql.ConnectionFactory;
import com.tree.finance.bigdata.utils.network.NetworkUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 13:48
 */
public class TaskManager {

    private final String queueNameUpdate;
    private final String queueNameInsert;
    private LinkedBlockingQueue<WriterRef> refs = new LinkedBlockingQueue<>(100);

    private static String SEP = ",";

    private static String QUOTE = "'";

    private volatile boolean stop = false;

    private Thread thread;

    private RabbitMqUtils rabbitMqUtils;

    private final String queueName;
    private final String taskTableName;
    private final Integer taskId;

    private static final Logger LOG = LoggerFactory.getLogger(TaskManager.class);

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ConnectionFactory connectionFactory;

    public TaskManager(int taskId) {
        this.taskId = taskId;
        this.queueName = PioneerConfig.getRabbitMqTaskQueue();
        //TODO not differentiate insert update delete
        this.queueNameUpdate = queueName + "_update";
        this.queueNameInsert = queueName + "_insert";

        this.rabbitMqUtils = RabbitMqUtils.getInstance(PioneerConfig.getRabbitHost(), PioneerConfig.getRabbitPort());
        this.thread = new Thread(this::sendTask, "Task-Generator");
        this.taskTableName = PioneerConfig.getTaskTable();
    }

    public void init() {
        this.connectionFactory = new ConnectionFactory.Builder().jdbcUrl(PioneerConfig.getTaskDbUrl())
                .user(PioneerConfig.getTaskDbUser()).password(PioneerConfig.getTaskDbPassword())
                .acquireIncrement(1).maxPollSize(1).minPollSize(1).initialPoolSize(1).build();
        this.thread.start();
    }

    public void commit(WriterRef ref) {
        while (!refs.offer(ref)) {
            try {
                LOG.warn("file queue is full !");
                Thread.sleep(2000l);
            }catch (InterruptedException e) {
                //no opt
            }
        }
    }

    public void sendTask() {

        while (!stop) {
            Random random = new Random();
            try {
                AvroWriterRef ref = (AvroWriterRef) refs.poll();
                if (ref == null) {
                    Thread.sleep(2000);
                    continue;
                }
                Path path = ref.getPath();
                Path pathSent = new Path(path.toString() + FileSuffix.sent.suffix());

                if (path.toString().endsWith(FileSuffix.tmp.suffix())) {
                    //todo alarm serious failure, no send unclosed file
                    LOG.error("not send unclosed file : {}", path);
                    continue;
                } else if (path.toString().endsWith(FileSuffix.sent.suffix())) {
                    LOG.warn("not send already sent file: {}", path);
                    continue;
                } else if (path.toString().endsWith(FileSuffix.done.suffix())) {

                    FileSystem fs = FileSystem.get(DfsConfigHolder.getConf());
                    fs.rename(path, pathSent);

                    String id = NetworkUtils.localIp + UUID.randomUUID().getLeastSignificantBits();

                    TaskInfo taskInfo = new TaskInfo(id, ref.getDb(), ref.getTable(), ref.getPartitionVals()
                            , ref.getPartitionName(), pathSent.toString(), ref.getOp());
                    String msgBody = JSON.toJSONString(taskInfo);

                    try {
                        writeToDb(taskInfo);
                        sendWithRetry(msgBody, taskInfo.getOp());
                    } catch (Exception e) {
                        LOG.error("failed to create file path: []", path, e);
                        fs.rename(pathSent, path);
                    }
                    LOG.info("create file task: [{}] success", pathSent);

                } else {
                    LOG.error("unknown file name: {}", path);
                }

            } catch (Throwable e) {
                //todo alarm serious failure
                LOG.error("may cause data inaccuracy", e);
            }
        }
        countDownLatch.countDown();
        LOG.info("stopped TaskManager");
    }

    private boolean sendWithRetry(String msgBody, Operation operation) {
        int retry = 0;
        while (retry++ < 3) {
            try {
                //TODO not differentiate insert update delete;
                if (operation.equals(Operation.CREATE)) {
                    rabbitMqUtils.produce(queueNameInsert, msgBody);
                } else {
                    rabbitMqUtils.produce(queueNameUpdate, msgBody);
                }
                return true;
            } catch (Exception e) {
                LOG.error("error send task", e);
            }
        }
        return false;
    }

    private void writeToDb(TaskInfo taskInfo) throws Exception {
        try (Connection conn = this.connectionFactory.getConnection();
             Statement stmt = conn.createStatement()) {
            StringBuilder sb = new StringBuilder("insert into ")
                    .append(taskTableName)
                    .append("(id, db, table_name, partition_name, file_path, op, status, create_time)")
                    .append("values (")
                    .append(QUOTE).append(taskInfo.getId()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(taskInfo.getDb()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(taskInfo.getTbl()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(taskInfo.getPartitionName()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(taskInfo.getFilePath()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(taskInfo.getOp().code()).append(QUOTE).append(SEP)
                    .append(QUOTE).append(TaskStatus.NEW).append(QUOTE).append(SEP)
                    .append(QUOTE).append(new DateTime().toString("yyyy-MM-dd HH:mm:ss")).append(QUOTE)
                    .append(")");
            stmt.execute(sb.toString());
        }
    }

    public void close() {
        LOG.info("stopping TaskManager...");
        while (!refs.isEmpty()) {
            try {
                Thread.sleep(2000);
                LOG.info("wait TaskManager to stop, remaining files in queue: {}", refs.size());
            } catch (InterruptedException e) {
                LOG.error("interrupted", e);
            }
        }
        stop = true;
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.connectionFactory.close();
        LOG.info("stopped TaskManager.");
    }

}
