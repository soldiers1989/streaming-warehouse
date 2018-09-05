package com.tree.finance.bigdata.task;

import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/4 20:12
 */
public class TaskInfo {

    private String id;
    private String db;
    private String tbl;
    private List<String> partitions;
    private String filePath;
    private Operation op;
    private String partitionName;
    //任务重试次数
    private Integer retryCount;
    //任务更新时间：创建时间、重试时间
    private Long lastUpdateTime;

    public TaskInfo(String id, String db, String tbl, List<String> partitions, String partitionName, String filePath, Operation op) {
        this.id = id;
        this.db = db;
        this.tbl = tbl;
        this.partitions = partitions;
        this.partitionName = partitionName;
        this.filePath = filePath;
        this.op = op;
        this.retryCount = 0;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Operation getOp() {
        return op;
    }

    public void setOp(Operation op) {
        this.op = op;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(Integer retryCount) {
        this.retryCount = retryCount;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
                "db='" + db + '\'' +
                ", tbl='" + tbl + '\'' +
                ", partitions=" + partitions +
                ", filePath='" + filePath + '\'' +
                ", op=" + op +
                ", partitionName='" + partitionName + '\'' +
                ", retryCount=" + retryCount +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
