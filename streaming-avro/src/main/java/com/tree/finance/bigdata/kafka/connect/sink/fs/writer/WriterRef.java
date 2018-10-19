package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.tree.finance.bigdata.kafka.connect.sink.fs.partition.PartitionHelper;
import com.tree.finance.bigdata.task.Operation;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;

import static com.tree.finance.bigdata.kafka.connect.sink.fs.partition.PartitionHelper.SINK_PARTITIONS_DEFAUL;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/6/25 17:27
 * @Modified By:
 */
public class WriterRef {
    //db
    private String db;
    //table
    private String table;

    //partition name
    private String partitionName;

    //bucket
    private int bucketId;
    //task id
    private int taskId;
    //operation: u, c, d, a
    private Operation op;

    private int version;

    //equals & hashcode method excluded fields
    private List<String> partitionVals;
    private Path path;

    public WriterRef(String db, String table, List<String> partitionVals, int bucketId, int taskId, Operation op, int
            version) {
        this.db = db;
        this.table = table;
        this.partitionVals = partitionVals;
        this.partitionName = PartitionHelper.getSinkPartitionName(SINK_PARTITIONS_DEFAUL, partitionVals);
        this.bucketId = bucketId;
        this.taskId = taskId;
        this.op = op;
        this.version = version;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getBucketId() {
        return bucketId;
    }

    public int getTaskId() {
        return taskId;
    }

    public Operation getOp() {
        return op;
    }

    public List<String> getPartitionVals() {
        return partitionVals;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WriterRef writerRef = (WriterRef) o;
        return bucketId == writerRef.bucketId &&
                taskId == writerRef.taskId &&
                Objects.equals(db, writerRef.db) &&
                Objects.equals(table, writerRef.table) &&
                Objects.equals(partitionName, writerRef.partitionName) &&
                op == writerRef.op &&
                version == writerRef.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, partitionName, bucketId, taskId, op, version);
    }
}
