package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.FileSuffix;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 13:48
 */
public class FileManager {

    private LinkedBlockingQueue<WriterRef> refs = new LinkedBlockingQueue<>(100);

    private SinkConfig sinkConfig;

    private static String SEP = ",";

    private static String QUOTE = "'";

    private volatile boolean stop = false;

    private Thread thread;

    private RabbitMqUtils rabbitMqUtils;

    private final String queueName;

    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ConnectionFactory connectionFactory;

    public FileManager(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.queueName = sinkConfig.getRabbitMqTaskQueue();
        this.rabbitMqUtils = RabbitMqUtils.getInstance(sinkConfig.getRabbitMqHost(), sinkConfig.getRabbitMqPort());
        this.thread = new Thread(this::sendTask, "task-sender");
    }

    public void init() {
        this.connectionFactory = new ConnectionFactory.Builder().jdbcUrl(sinkConfig.getTaskDbUrl())
                .user(sinkConfig.getTaskDbUser()).password(sinkConfig.getTaskDbPassword())
                .acquireIncrement(3).maxPollSize(10).minPollSize(1).initialPoolSize(1).build();
        this.thread.start();
    }

    public void commit(WriterRef ref) {
        refs.offer(ref);
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

                    String id = System.currentTimeMillis() + "-" + NetworkUtils.localIp + "-" + sinkConfig.getTaskId()
                            + "-" + random.nextInt(100);

                    TaskInfo taskInfo = new TaskInfo(id, ref.getDb(), ref.getTable(), ref.getPartitionVals()
                            , ref.getPartitionName(), pathSent.toString(), ref.getOp());
                    String msgBody = JSON.toJSONString(taskInfo);

                    try {
                        writeToDb(taskInfo);
                        sendWithRetry(msgBody);
                    }catch (Exception e) {
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
        LOG.info("FileManager stopped");
    }

    private boolean sendWithRetry(String msgBody) {
        int retry = 0;
        while (retry++ < 3) {
            try {
                rabbitMqUtils.produce(queueName, msgBody);
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
                    .append(sinkConfig.getTaskTable())
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
        while (!refs.isEmpty()) {
            try {
                Thread.sleep(2000);
                LOG.info("waiting FileManager to stop, remaining files in queue: {}", refs.size());
            } catch (InterruptedException e) {
                LOG.error("interrupted", e);
            }
        }
        stop = true;
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("interrupted");
        }
        this.connectionFactory.close();
    }

}
