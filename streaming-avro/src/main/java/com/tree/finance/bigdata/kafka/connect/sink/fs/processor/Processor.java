package com.tree.finance.bigdata.kafka.connect.sink.fs.processor;

import com.tree.finance.bigdata.kafka.connect.sink.fs.config.SinkConfig;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.AvroSchemaClient;
import com.tree.finance.bigdata.kafka.connect.sink.fs.convertor.ValueConvertor;
import com.tree.finance.bigdata.kafka.connect.sink.fs.partition.PartitionHelper;
import com.tree.finance.bigdata.kafka.connect.sink.fs.schema.VersionedTable;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.Writer;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterFactory;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterManager;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.WriterRef;
import com.tree.finance.bigdata.kafka.connect.sink.fs.writer.avro.AvroWriterRef;
import com.tree.finance.bigdata.task.Operation;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

import static com.tree.finance.bigdata.schema.SchemaConstants.*;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/6/29 11:33
 */
public class Processor {

    private SinkConfig config;

    private WriterFactory writerFactory;

    private PartitionHelper partitionHelper;

    private String dbName;

    private static Logger LOG = LoggerFactory.getLogger(Processor.class);

    public Processor(SinkConfig config) {
        this.partitionHelper = new PartitionHelper(config);
        this.config = config;
        this.writerFactory = new WriterManager(config);
        this.dbName = config.getHiveDestinationDbName();
    }

    public void init() throws Exception {
        writerFactory.init();
    }

    private void writeRecord(SinkRecord sinkRecord) throws Exception {
        Writer<GenericData.Record> writer = null;
        try {
            WriterRef ref = getAvroWriterRef(sinkRecord);
            //when partition value is not set for update、delete operation
            if (null == ref) {
                return;
            }
            writer = writerFactory.getOrCreate(ref);
            //one connector on database
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
//        String dbName = DatabaseUtils.getConvertedDb(String.valueOf(source.get(FIELD_DB)));
        String tableName = String.valueOf(source.get(FIELD_TABLE));
        Struct after = (Struct) struct.get(FIELD_AFTER);
        if (after == null) {
            after = (Struct) struct.get(FIELD_BEFORE);
        }
        Integer version = sinkRecord.valueSchema().version();
        String op = ((Struct) sinkRecord.value()).getString(FIELD_OP);

        List<String> sourceParCols = partitionHelper.getSourcePartitionCols(new VersionedTable(dbName, tableName, version), after);
        List<String> partitionVals = partitionHelper.buildYmdPartitionVals(sourceParCols.get(0), after);

        //if partition column has no value，then partition by current time for insert, ignore for update and delete.
        if (null == partitionVals) {
            if (Operation.CREATE.equals(Operation.forCode(op))) {
                partitionVals = new ArrayList<>();
                Calendar calendar = Calendar.getInstance();
                partitionVals.add(Integer.toString(calendar.get(Calendar.YEAR)));
                partitionVals.add(Integer.toString(calendar.get(Calendar.MONTH)));
                partitionVals.add(Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)));
            } else {
                LOG.warn("no partition value found, table: {}.{}, record: {}", dbName, tableName, after);
                return null;
            }
        }

        VersionedTable versionedTable = new VersionedTable(dbName, tableName, version);

        return new AvroWriterRef(dbName, tableName, partitionVals, 1, config.getTaskId(),
                Operation.forCode(op), AvroSchemaClient.getSchema(versionedTable, sinkRecord), version);
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
