package com.tree.finance.bigdata.hive.streaming.mutation;

import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockFailureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/7/10 20:08
 */
public class HiveLockFailureListener implements LockFailureListener {

    private Logger LOG = LoggerFactory.getLogger(HiveLockFailureListener.class);

    public HiveLockFailureListener(){
    }

    @Override
    public void lockFailed(long lockId, Long transactionId, Iterable<String> tableNames, Throwable t) {
        //只有在transaction.abort和commit释放锁时，才会调用该回调方法。任务已经做了失败处理
        StringBuilder tbls = new StringBuilder();
        for (String s: tableNames){
            tbls.append(s).append(',');
        }
        LOG.error("unlock failed, transaction: [" + transactionId + "] table: ["+ tbls.toString() + "]", t);
    }
}
