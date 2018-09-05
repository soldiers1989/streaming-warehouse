package com.tree.finance.bigdata.hive.streaming.service;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:10
 */
public interface Service {

    void stop() throws InterruptedException;

    void init() throws Exception;

}
