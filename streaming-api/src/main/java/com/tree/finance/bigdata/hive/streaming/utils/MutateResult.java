package com.tree.finance.bigdata.hive.streaming.utils;

public class MutateResult {

    private long update = 0;

    private long insert = 0;

    private long delete = 0;

    public long getUpdate() {
        return update;
    }

    public long getInsert() {
        return insert;
    }

    public long getDelete() {
        return delete;
    }

    public void incUpdate(){
        update ++;
    }

    public void incDelete(){
        delete ++;
    }

    public void incInsert() {
        insert ++;
    }

}
