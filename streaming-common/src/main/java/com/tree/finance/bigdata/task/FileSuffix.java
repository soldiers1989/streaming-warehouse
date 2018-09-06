package com.tree.finance.bigdata.task;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/12 15:17
 */
public enum  FileSuffix {
    tmp(".tmp"),
    done(".done"),
    sending(".sending"),
    sent(".sent");

    private String suffix;

    FileSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String suffix(){
        return this.suffix;
    }
}
