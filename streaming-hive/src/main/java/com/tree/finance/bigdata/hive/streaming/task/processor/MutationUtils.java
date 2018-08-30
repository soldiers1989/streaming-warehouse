package com.tree.finance.bigdata.hive.streaming.task.processor;

import org.apache.hive.hcatalog.streaming.mutate.client.MutatorClient;
import org.apache.hive.hcatalog.streaming.mutate.client.Transaction;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorCoordinator;
import org.apache.hive.hcatalog.streaming.mutate.worker.MutatorFactory;

import java.io.IOException;

/**
 * @author Zhengsj
 * Description:
 * Created in 2018/8/27 14:24
 */
public class MutationUtils {

    private MutatorClient mutatorClient;

    private MutatorCoordinator mutateCoordinator;

    private Transaction mutateTransaction;

    private MutatorFactory factory;

    private long rowId = -1;

    private boolean initialized = false;

    public MutatorClient getMutatorClient() {
        return mutatorClient;
    }

    public void setMutatorClient(MutatorClient mutatorClient) {
        this.mutatorClient = mutatorClient;
    }

    public MutatorCoordinator getMutateCoordinator() {
        return mutateCoordinator;
    }

    public void setMutateCoordinator(MutatorCoordinator mutateCoordinator) {
        this.mutateCoordinator = mutateCoordinator;
    }

    public Transaction getMutateTransaction() {
        return mutateTransaction;
    }

    public void setMutateTransaction(Transaction mutateTransaction) {
        this.mutateTransaction = mutateTransaction;
    }

    public MutatorFactory getFactory() {
        return factory;
    }

    public void setFactory(MutatorFactory factory) {
        this.factory = factory;
    }

    public long incAndReturnRowId() {
        rowId++;
        return rowId;
    }

    public void setInitialized() {
        this.initialized = true;
    }

    public boolean initialized() {
        return initialized;
    }

    public void closeMutator() throws IOException {
        if (null != this.mutateCoordinator) {
            this.mutateCoordinator.close();
        }
    }

    public void commitTransaction() throws Exception {
        if (null != mutateTransaction) {
            mutateTransaction.commit();
        }
    }

    public void closeMutatorQuietly() {
        try {
            if (null != this.mutateCoordinator) {
                this.mutateCoordinator.close();
            }
        } catch (Exception e) {
            //no opt
        }
    }

    public void abortTxnQuietly() {
        try {
            if (mutateTransaction != null) {
                mutateTransaction.abort();
            }
        } catch (Exception e) {
            //no pot
        }
    }

    public void closeClientQueitely() {
        try {
            if (null != mutatorClient) {
                mutatorClient.close();
            }
        } catch (Exception e) {
            //no pots
        }
    }
}
