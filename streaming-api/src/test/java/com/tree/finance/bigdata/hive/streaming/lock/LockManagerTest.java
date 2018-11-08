package com.tree.finance.bigdata.hive.streaming.lock;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LockManagerTest {

    private AtomicInteger aquried = new AtomicInteger(0);

    @Test
    public void getLock(){
        LockComponent lockComponent = new LockComponent("test", "test", Lists.newArrayList("p_y=2018", "p_m=09", "p_d=11"));
        try {
            LockManager.getSingeleton().getLock(lockComponent, 20, TimeUnit.SECONDS);
            System.out.println("aquried lock: " + Thread.currentThread().getName());
            Assert.assertEquals(1, aquried.incrementAndGet());
            Thread.sleep(5000);
            aquried.decrementAndGet();
            LockManager.getSingeleton().releaseLock(lockComponent);
        }catch (Exception e ) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConccurency() throws Exception{
        Thread thread = new Thread(this::getLock, "t1");
        thread.start();

        Thread thread2 = new Thread(this::getLock, "t2");
        thread2.start();

        Thread thread3 = new Thread(this::getLock, "t3");
        thread3.start();

        thread.join();
        thread2.join();
        thread3.join();
    }

    @After
    public void close(){
        LockManager.getSingeleton().close();
    }
}
