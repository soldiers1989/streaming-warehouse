package com.tree.finance.bigdata.hive.streaming.task.processor;

import java.util.Objects;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/26 20:55
 */
public class LockComponent {

    private String db;
    private String table;
    private String partition;

    public LockComponent(String db, String table, String partition) {
        this.db = db;
        this.table = table;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LockComponent that = (LockComponent) o;
        return Objects.equals(db, that.db) &&
                Objects.equals(table, that.table) &&
                Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table, partition);
    }
}
