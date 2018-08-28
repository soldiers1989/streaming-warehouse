package com.tree.finance.bigdata.kafka.connect.sink.fs.processor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.AvroSchemaClient;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.ValueConvertor;
import com.tree.finance.bigdata.kafka.connect.sink.fs.partition.PartitionHelper;
import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import com.tree.finance.bigdata.kafka.connect.sink.fs.utils.DatabaseUtils;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.Writer;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterFactory;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterManager;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import static com.tree.finance.bigdata.schema.SchemaConstants.*;

import java.util.Collection;
import java.util.List;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/6/29 11:33
 */
public class Processor {

    private SinkConfig config;

    private WriterFactory writerFactory;

    private PartitionHelper partitionHelper;

    public Processor(SinkConfig config) {
        this.partitionHelper = new PartitionHelper(config);
        this.config = config;
        this.writerFactory = new WriterManager(config);
    }

    public void init() throws Exception {
        writerFactory.init();
    }

    private void writeRecord(SinkRecord sinkRecord) throws Exception {
        Writer<GenericData.Record> writer = null;
        try {
            WriterRef ref = getAvroWriterRef(sinkRecord);
            writer = writerFactory.getOrCreate(ref);
            writer.write(ValueConvertor.connectToGeneric(((AvroWriterRef)ref).getSchema(), sinkRecord));
        } finally {
            if (writer != null) {
                writer.unlock();
            }
        }
    }

    private WriterRef getAvroWriterRef(SinkRecord sinkRecord) throws Exception {
        Struct struct = (Struct) sinkRecord.value();
        if (struct == null) {
            return null;
        }
        Struct source = (Struct) struct.get(FIELD_SOURCE);
        String dbName = DatabaseUtils.getConvertedDb(String.valueOf(source.get(FIELD_DB)));
        String tableName = String.valueOf(source.get(FIELD_TABLE));
        Struct after = (Struct) struct.get(FIELD_AFTER);
        if (after == null) {
            after = (Struct) struct.get(FIELD_BEFORE);
        }
        Integer version = sinkRecord.keySchema().version();
        String op = ((Struct) sinkRecord.value()).getString(FIELD_OP);

        List<String> sourceParCols = partitionHelper.getSourcePartitionCols(new VersionedTable(dbName, tableName, version), after);
        List<String> partitionVals = partitionHelper.buildYmdPartitionVals(sourceParCols.get(0), after);

        VersionedTable versionedTable = new VersionedTable(dbName, tableName, sinkRecord.valueSchema().version());

        return new AvroWriterRef(dbName, tableName, partitionVals, 1, config.getTaskId(),
                Operation.forCode(op), AvroSchemaClient.getSchema(versionedTable, sinkRecord));
    }

    public void process(Collection<SinkRecord> records) throws Exception{
        for (SinkRecord record : records){
            writeRecord(record);
        }
    }

    public void stop() {
        this.writerFactory.close();
    }
}
