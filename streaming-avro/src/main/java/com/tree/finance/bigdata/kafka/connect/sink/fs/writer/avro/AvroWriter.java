package com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.DfsConfigHolder;
import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.Writer;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.task.FileSuffix;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.OutputStream;
import java.util.List;

/**
 * @Author ZhengShengJun
 * @Description
 * @Date Created in 2018/6/28 14:23
 * @Modified By:
 */
public class AvroWriter extends Writer<GenericData.Record> {

    private DataFileWriter<Object> dataFileWriter;

    private OutputStream outputStream;

    private FileSystem fs;

    private Path path;

    public AvroWriter(WriterRef ref, SinkConfig config) {
        super(ref, config);
        this.path = makePath();
        ref.setPath(path);
    }

    @Override
    public void write(GenericData.Record record) throws Exception {
        try {
            wroteMsg ++;
            dataFileWriter.append(record);
        } catch (Exception e) {
            LOG.error("write failed file path: {} \nwrite schema: {}\ndata schema: {}\ndata: {}",
                    path, ((AvroWriterRef) ref).getSchema(), record.getSchema(), record.toString(), e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void write(List<GenericData.Record> t) throws Exception {
        for (GenericData.Record record : t) {
            write(record);
        }
    }

    @Override
    public void init() throws Exception {
        dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
        this.fs = FileSystem.get(DfsConfigHolder.getConf());
        this.outputStream = fs.create(path);
        LOG.info("created writer file: {}", path);
        dataFileWriter.create(((AvroWriterRef) ref).getSchema(), outputStream);
    }

    @Override
    public void closeInternal() {
        try {
            if (dataFileWriter != null) {
                dataFileWriter.close();
            }
            Path finalPath = new Path(path.toString().replaceAll(FileSuffix.tmp.suffix(), FileSuffix.done.suffix()));
            fs.rename(path, finalPath);
            ref.setPath(finalPath);
            LOG.info("closed writer: {}", path);
        } catch (Exception e) {
            //todo alarm
            LOG.error("error close writer", e);
        }
    }

}
