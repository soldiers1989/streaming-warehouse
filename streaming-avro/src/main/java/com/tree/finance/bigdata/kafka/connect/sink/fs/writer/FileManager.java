package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.alibaba.fastjson.JSON;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.utils.mq.RabbitMqUtils;
import com.tree.finance.bigdata.task.FileSuffix;
import com.tree.finance.bigdata.task.TaskInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private volatile boolean stop = false;

    private Thread thread;

    private RabbitMqUtils rabbitMqUtils;

    private final String queueName;

    private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    public FileManager(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.queueName = sinkConfig.getRabbitMqTaskQueue();
        this.rabbitMqUtils = RabbitMqUtils.getInstance(sinkConfig.getRabbitMqHost(), sinkConfig.getRabbitMqPort());
        this.thread = new Thread(this::sendTask, "task-sender");
    }

    public void init() {
        this.thread.start();
    }

    public void commit(WriterRef ref) {
        refs.offer(ref);
    }

    public void sendTask() {

        while (!stop) {
            try {
                AvroWriterRef ref = (AvroWriterRef) refs.poll();
                if (ref == null){
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

                    TaskInfo taskInfo = new TaskInfo(ref.getDb(), ref.getTable(), ref.getPartitionVals()
                            , ref.getPartitionName(), pathSent.toString(), ref.getOp());
                    String msgBody = JSON.toJSONString(taskInfo);

                    if (sendWithRetry(msgBody)) {
                        LOG.info("sent file: [{}] to mq success", pathSent);
                    } else {
                        //todo alarm can't send task to queue
                        fs.rename(pathSent, path);
                    }

                } else {
                    //todo alarm unkonw file
                    LOG.error("unknown file name: {}", path);
                }

            } catch (Throwable e) {
                //todo alarm serious failure
                LOG.error("", e);
            }
        }
        countDownLatch.countDown();
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

    public void close() {
        while (!refs.isEmpty()){
            try {
                Thread.sleep(2000);
                LOG.info("waiting FileManager to stop, remaining files in queue: {}", refs.size());
            }catch (InterruptedException e){
                LOG.error("interrupted", e);
            }
        }
        stop = true;
        while (countDownLatch.getCount() != 0) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("interrupted");
            }
        }

    }

}
