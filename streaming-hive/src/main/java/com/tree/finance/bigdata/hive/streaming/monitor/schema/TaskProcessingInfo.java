package com.tree.finance.bigdata.hive.streaming.monitor.schema;

import java.util.Date;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/26 19:13
 */
public class TaskProcessingInfo {

    private String threadId;

    private Date startTime;

    public TaskProcessingInfo(String threadId, Date startTime) {
        this.threadId = threadId;
        this.startTime = startTime;
    }

    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
}
