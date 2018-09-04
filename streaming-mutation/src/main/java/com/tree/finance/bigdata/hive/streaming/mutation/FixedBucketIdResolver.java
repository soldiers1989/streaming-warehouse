package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.hive.hcatalog.streaming.mutate.worker.BucketIdResolver;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/3 20:10
 */
public class FixedBucketIdResolver implements BucketIdResolver {

    @Override
    public Object attachBucketIdToRecord(Object record) {
        return null;
    }

    @Override
    public int computeBucketId(Object record) {
        return 0;
    }
}
