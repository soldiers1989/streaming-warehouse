package com.tree.finance.bigdata.hive.streaming.lock;

import java.util.List;
import java.util.Objects;

public class LockComponent {

    private String db;

    private String table;

    private String partition = "";

    public LockComponent(String db, String table, List<String> partitions) {
        this.db = db;
        this.table = table;
        partitions.forEach(p -> partition += p);
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

    public String getLockPath() {
        return "/" + db + "/" + table + "." + partition;
    }
}
