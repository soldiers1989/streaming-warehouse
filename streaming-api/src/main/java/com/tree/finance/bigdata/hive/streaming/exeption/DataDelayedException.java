package com.tree.finance.bigdata.hive.streaming.exeption;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/23 09:41
 */
public class DataDelayedException extends Exception{
    public DataDelayedException(String msg){
        super(msg);
    }
}
