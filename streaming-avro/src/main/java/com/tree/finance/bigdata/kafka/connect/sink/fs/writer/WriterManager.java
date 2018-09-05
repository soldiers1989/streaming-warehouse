package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
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

    private Constructor constructor;

    private FileManager fileManager;

    private int maxOpenFilesPerTask;

    public WriterManager(SinkConfig config) {
        super(config);
        this.checkThread = new Thread(this::startCheck, "writer-check-" + config.getTaskId());
        this.fileManager = new FileManager(config);

    }

    @Override
    public void init() throws Exception {
        running = true;
        Class writerClass = Class.forName(config.getWriterClass());
        this.constructor = writerClass.getConstructor(WriterRef.class, SinkConfig.class);
        this.countDownLatch = new CountDownLatch(1);
        this.checkThread.start();
        this.fileManager.init();
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
                    fileManager.commit(writer.ref);

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
        Writer writer = (Writer) constructor.newInstance(writerRef, config);
        writer.init();
        return writer;
    }

    @Override
    public void close() {
        this.running = false;
        this.checkThread.interrupt();
        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            LOG.warn("interrupted while stopping");
        }
        writerMap.forEach(this::closeImmedidately);
        LOG.info("WriterManager closed");
        this.fileManager.close();
    }

    private void startCheck() {
        while (running) {
            try {
                writerMap.forEach(this::closeIfExpired);
                Thread.sleep(5000);
            } catch (Throwable t) {
                LOG.error("error checking writers", t);
            }
        }
        LOG.info("check thread stopped...");
    }

    private void closeIfExpired(WriterRef ref, Writer writer) {
        try {
            if (writer.isExpired()) {
                writerMap.remove(ref);
                writer.close();
                fileManager.commit(ref);
            }
        } catch (Exception e) {
            LOG.error("failed to check writer expiration", e);
        }
    }

    private void closeImmedidately(WriterRef ref, Writer writer) {
        try {
            writerMap.remove(ref);
            writer.close();
            fileManager.commit(ref);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }
}
