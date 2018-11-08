package com.tree.finance.bigdata.hive.streaming.lock;

import com.tree.finance.bigdata.hive.streaming.constants.ConfigFactory;
import com.tree.finance.bigdata.hive.streaming.exeption.DistributeLockException;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hive.hcatalog.streaming.mutate.client.TransactionException;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LockManager {

    private static Logger LOG = LoggerFactory.getLogger(LockManager.class);

    private volatile boolean initialized = false;

    private static ThreadLocal<Map<LockComponent, InterProcessMutex>> locks = new ThreadLocal<>();

    private CuratorFramework cf;

    private LockManager() {
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 5);
            cf = CuratorFrameworkFactory.builder()
                    .connectString(ConfigFactory.getHiveLockZkQuorum())
                    .namespace(ConfigFactory.getHiveLockZkParent())
                    .sessionTimeoutMs(ConfigFactory.getHiveZkSessionTimeOut())
                    .retryPolicy(retryPolicy)
                    .build();
            cf.start();
            initialized = true;
        } catch (Exception e) {
            LOG.error("failed to init LockManager", e);
            throw new RuntimeException();
        }
    }

    public void close() {
        cf.close();
    }

    private static class LockManagerHolder {
        static LockManager instance = new LockManager();
    }

    public static LockManager getSingeleton() {
        return LockManagerHolder.instance;
    }

    public boolean getLock(LockComponent lockComponent, int timeOut, TimeUnit unit) throws DistributeLockException {
        try {
            if (locks.get() == null) {
                locks.set(new HashMap<>());
            }
            if (locks.get().get(lockComponent) != null) {
                LOG.warn("lock reentrant, should not hannpen !");
                return locks.get().get(lockComponent).isAcquiredInThisProcess();
            }
            long start = System.currentTimeMillis();
            InterProcessMutex lock = new InterProcessMutex(cf, lockComponent.getLockPath());
            if (lock.acquire(timeOut, TimeUnit.SECONDS)) {
                locks.get().put(lockComponent, lock);
                LOG.info("zk lock cost: {}ms", System.currentTimeMillis() - start);
                return true;
            } else {
                LOG.info("zk lock timeout cost: {}ms", System.currentTimeMillis() - start);
                throw new DistributeLockException("lock time out");
            }
        }catch (Exception e) {
            LOG.error("error getting lock", e);
            throw new DistributeLockException("lock failed");
        }
    }

    public void releaseLock(LockComponent lockComponent) {
        if (locks.get() == null) {
            return ;
        }
        InterProcessMutex lock = locks.get().get(lockComponent);
        if (lock != null) {
            locks.get().remove(lockComponent);
            releaseInternal(lock, lockComponent);
        }
    }

    private void releaseInternal(InterProcessMutex lock, LockComponent lockComponent) {
        try {
            if (lock.isAcquiredInThisProcess()){
                lock.release();
            }
        } catch (Exception e) {
            LOG.error("failed to release lock, ignore", e);
        }
        //clean zk path
        try {
            cf.delete().forPath(lockComponent.getLockPath());
        }catch (Exception e) {
            //ignore
        }
    }

}
