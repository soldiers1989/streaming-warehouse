package com.tree.finance.bigdata.kafka.connect.sink.fs.schema;

import java.util.Objects;

/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/26 16:59
 */
public class VersionedTable {

    private String db;
    private String table;
    private int version;

    public VersionedTable(String db, String table, int version) {
        this.db = db;
        this.table = table;
        this.version = version;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedTable that = (VersionedTable) o;
        return version == that.version &&
                Objects.equals(db, that.db) &&
                Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {

        return Objects.hash(db, table, version);
    }

    @Override
    public String toString() {
        return db + "." + table + ".v" + version;
    }
}
