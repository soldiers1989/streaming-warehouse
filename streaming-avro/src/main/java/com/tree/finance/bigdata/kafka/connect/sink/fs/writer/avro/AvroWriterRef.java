package com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro;

import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.Schema;

import java.util.List;

/**
 * @author ZhengShengJun
 * Description
 * Created in 2018/6/28 16:06
 */
public class AvroWriterRef extends WriterRef {

    private Schema schema;

    public AvroWriterRef (String db, String table, List<String> partitionVals, int bucketId, int taskId, Operation op, Schema schema, int version){
        super(db, table, partitionVals, bucketId, taskId, op, version);
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

}
