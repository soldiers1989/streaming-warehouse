package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.task.FileSuffix;
import com.tree.finance.bigdata.task.Operation;
import com.tree.finance.bigdata.utils.network.NetworkUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig.Default.DFS_FILE_SEPARATOR;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/6/25 16:59
 * @Modified By:
 */
public abstract class Writer<T> {

    protected WriterRef ref;

    protected long wroteMsg;
    protected long maxInsertMsgPerFile;
    protected long maxUpdateMsgPerFile;

    protected long createTime = System.currentTimeMillis();
    protected long ttlMillSec;

    protected Logger LOG = LoggerFactory.getLogger(Writer.class);

    protected volatile boolean closed = false;

    protected SinkConfig config;

    private volatile boolean writing = false;

    protected Writer(WriterRef ref, SinkConfig config) {
        this.config = config;
        this.ref = ref;
        this.maxInsertMsgPerFile = config.getWriterMaxInsertMsg();
        this.maxUpdateMsgPerFile = config.getWriterMaxUpdateMsg();
        this.ttlMillSec = config.getWriterTTLMin() * 1000 * 60;
    }

    public abstract void write(T t) throws Exception;

    public abstract void write(List<T> t) throws Exception;

    public abstract void init() throws Exception;

    public boolean isExpired() {

        if (ref.getOp().equals(Operation.CREATE)) {
            if (wroteMsg > maxInsertMsgPerFile) {
                return true;
            }
        } else if (ref.getOp().equals(Operation.UPDATE) || ref.getOp().equals(Operation.DELETE)) {
            if (wroteMsg > maxUpdateMsgPerFile) {
                return true;
            }
        } else {
          throw new RuntimeException("unknown operation type: " + ref.getOp());
        }

        return  System.currentTimeMillis() - createTime > ttlMillSec;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * path: db/tbl/bucket/taskId/insert or update/write.tmp
     **/
    protected Path makePath() {
        StringBuilder sb = new StringBuilder(config.getWriterBasePath());
        sb.append(DFS_FILE_SEPARATOR).append(ref.getOp())
                .append(DFS_FILE_SEPARATOR).append(ref.getDb())
                .append(DFS_FILE_SEPARATOR).append(ref.getTable())
                .append(DFS_FILE_SEPARATOR).append(ref.getPartitionName())
                .append(DFS_FILE_SEPARATOR).append(ref.getBucketId())
                .append(DFS_FILE_SEPARATOR).append(ref.getTaskId())
                .append('-').append(NetworkUtils.localIp)
                .append('-').append(System.currentTimeMillis()).append(FileSuffix.tmp.suffix());
        return new Path(sb.toString());
    }

    public synchronized boolean tryLock() {
        if (closed == false) {
            this.writing = true;
            return true;
        } else {
            return false;
        }
    }

    abstract public void closeInternal();

    public void close() throws Exception {
        while (!canClose()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.warn("interrupted");
            }
        }
        closeInternal();
        this.closed = true;
    }

    public synchronized boolean canClose() {
        if (!writing) {
            closed = true;
            return true;
        } else {
            return false;
        }
    }

    public synchronized void unlock() {
        this.writing = false;
    }
}
