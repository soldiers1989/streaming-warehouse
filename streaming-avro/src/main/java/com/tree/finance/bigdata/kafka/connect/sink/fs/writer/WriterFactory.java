package com.tree.finance.bigdata.kafka.connect.sink.fs.writer;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/6/25 17:00
 * @Modified By:
 */
public abstract class WriterFactory {

    public abstract void init() throws Exception;

    public abstract Writer getOrCreate(WriterRef writerRef) throws Exception;

    protected abstract Writer create(WriterRef writerRef) throws Exception;

    public abstract void close();
}
