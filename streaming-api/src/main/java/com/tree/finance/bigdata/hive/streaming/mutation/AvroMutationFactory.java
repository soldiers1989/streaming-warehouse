package com.tree.finance.bigdata.hive.streaming.mutation;

import com.tree.finance.bigdata.hive.streaming.mutation.inspector.AvroObjectInspector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.hcatalog.streaming.mutate.worker.*;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 17:17
 */
public class AvroMutationFactory implements MutatorFactory {

    private Configuration configuration;

    private AvroObjectInspector objectInspector;

    private final int FIXED_RECORD_ID_COLUMN = 0;

    public AvroMutationFactory(Configuration configuration, AvroObjectInspector objectInspector){
        this.configuration = configuration;
        this.objectInspector = objectInspector;
    }

    public AvroObjectInspector getObjectInspector() {
        return objectInspector;
    }

    @Override
    public Mutator newMutator(AcidOutputFormat<?, ?> outputFormat, long transactionId, Path partitionPath, int bucketId) throws IOException {
        return new MutatorImpl(configuration, FIXED_RECORD_ID_COLUMN, objectInspector, outputFormat, transactionId, partitionPath,
                bucketId);
    }

    @Override
    public RecordInspector newRecordInspector() {
        return new AvroRecordInspector();
    }

    @Override
    public BucketIdResolver newBucketIdResolver(int totalBuckets) {
        return new FixedBucketIdResolver();
    }
}
