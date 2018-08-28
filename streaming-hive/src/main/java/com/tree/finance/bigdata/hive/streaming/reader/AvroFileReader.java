package com.tree.finance.bigdata.hive.streaming.reader;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 11:11
 */
public class AvroFileReader implements AutoCloseable {

    private Path path;

    private DataFileReader reader;

    private Logger LOG = LoggerFactory.getLogger(AvroFileReader.class);

    public AvroFileReader(Path path) {
        this.path = path;
        init();

    }

    public void init() {
        try {
            this.reader = new DataFileReader(new FsInput(path, new Configuration()), new GenericDatumReader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Schema getSchema() {
        return reader.getSchema();
    }

    public GenericData.Record next() {
        return (GenericData.Record) reader.next();
    }

    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (Exception e) {
            LOG.error("error closing avro reader", e);
        }
    }
}
