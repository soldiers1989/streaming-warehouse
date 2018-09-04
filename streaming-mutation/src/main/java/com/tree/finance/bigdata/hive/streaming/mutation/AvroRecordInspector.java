package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hive.hcatalog.streaming.mutate.worker.RecordInspector;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/2 14:10
 */
public class AvroRecordInspector implements RecordInspector {

    @Override
    public RecordIdentifier extractRecordIdentifier(Object record) {
        return new RecordIdentifier(-1, 0, -1);
    }

}
