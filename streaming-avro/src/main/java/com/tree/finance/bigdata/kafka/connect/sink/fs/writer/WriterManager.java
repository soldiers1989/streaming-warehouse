package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/25 16:59
 */
public class WriterManager extends WriterFactory {

    private ConcurrentHashMap<WriterRef, Writer> writerMap = new ConcurrentHashMap<>();

    private Logger LOG = LoggerFactory.getLogger(WriterManager.class);

    private Thread checkThread;

    private volatile boolean running;

    private CountDownLatch countDownLatch;

    private TaskManager taskManager;

    private int maxOpenFilesPerTask;

    private String db;

    private int id;

    public WriterManager(String db, int id) {
        this.checkThread = new Thread(this::startCheck, "writer-check-" + id);
        this.taskManager = new TaskManager(id);
        this.db = db;
        this.id = id;
    }

    @Override
    public void init() throws Exception {
        running = true;
        this.countDownLatch = new CountDownLatch(1);
        this.checkThread.start();
        this.taskManager.init();
    }

    @Override
    public Writer getOrCreate(WriterRef writerRef) throws Exception {

        if (writerMap.containsKey(writerRef)) {
            Writer writer = writerMap.get(writerRef);

            // if writer is closed by writer-check thread
            if (!writer.tryLock()) {
                Writer newWriter = create(writerRef);
                newWriter.tryLock();
                writerMap.put(writerRef, newWriter);
            } else {
                //check writer exceed max file
                if (!writer.isExpired()) {
                    return writer;
                } else {
                    //remove and unlock and close
                    writerMap.remove(writerRef);
                    writer.unlock();
                    writer.close();

                    //should not commit parameter writer ref， which has no path
                    taskManager.commit(writer.ref);

                    Writer newWriter = create(writerRef);
                    newWriter.tryLock();
                    writerMap.put(writerRef, newWriter);
                }
            }
        } else {
            Writer writer = create(writerRef);
            writer.tryLock();
            writerMap.put(writerRef, writer);
        }
        return writerMap.get(writerRef);
    }

    @Override
    protected Writer create(WriterRef writerRef) throws Exception {
        Writer writer = new AvroWriter(writerRef);
        writer.init();
        return writer;
    }

    @Override
    public void close() {
        LOG.info("stooping WriteManager {}-{}", db, id);
        this.running = false;
        this.checkThread.interrupt();
        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            LOG.warn("interrupted while await countdown latch");
        }
        LOG.info("stopped check thread");
        writerMap.forEach(this::closeImmedidately);
        LOG.info("All writing files closed");
        this.taskManager.close();
    }

    private void startCheck() {
        while (running) {
            try {
                writerMap.forEach(this::closeIfExpired);
                Thread.sleep(5000);
            }catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.error("error checking writers", t);
            }
        }
        countDownLatch.countDown();
        LOG.info("stopped check thread...");
    }

    private void closeIfExpired(WriterRef ref, Writer writer) {
        try {
            if (writer.isExpired()) {
                writerMap.remove(ref);
                writer.close();
                taskManager.commit(ref);
            }
        } catch (Exception e) {
            LOG.error("failed to check writer expiration", e);
        }
    }

    private void closeImmedidately(WriterRef ref, Writer writer) {
        try {
            writerMap.remove(ref);
            writer.close();
            taskManager.commit(ref);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}
