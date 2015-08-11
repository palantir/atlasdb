package com.palantir.atlas.impl;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.TxTask;
import com.palantir.common.base.Throwables;

public class TransactionRunner implements Runnable {
    private static final TransactionRequest<Object> COMMIT_SENTINEL = new TransactionRequest<Object>(null);
    private static final TransactionRequest<Object> ABORT_SENTINEL = new TransactionRequest<Object>(null);
    private final SynchronousQueue<TransactionRequest<Object>> channel = new SynchronousQueue<TransactionRequest<Object>>();
    private final TransactionManager txManager;
    private final AtomicBoolean isFinished = new AtomicBoolean();

    public TransactionRunner(TransactionManager txManager) {
        this.txManager = txManager;
    }

    @Override
    public void run() {
        txManager.runTaskThrowOnConflict(new TxTask() {
            @Override
            public Void execute(Transaction t) {
                while (true) {
                    try {
                        TransactionRequest<Object> request = channel.take();
                        if (request == COMMIT_SENTINEL) {
                            return null;
                        } else if (request == ABORT_SENTINEL) {
                            t.abort();
                            return null;
                        }
                        Object result = request.getTask().execute(t);
                        request.send(result);
                    } catch (InterruptedException e) {
                        throw Throwables.rewrapAndThrowUncheckedException(e);
                    }
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> T submit(RuntimeTransactionTask<T> task) {
        Preconditions.checkNotNull(channel, "This transaction has already been completed.");
        TransactionRequest<T> request = new TransactionRequest<T>(task);
        try {
            TransactionRequest<Object> cast = (TransactionRequest<Object>) request;
            channel.put(cast);
            return request.receive();
        } catch (InterruptedException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    public void commit() {
        finish(COMMIT_SENTINEL);
    }

    public void abort() {
        finish(ABORT_SENTINEL);
    }

    private void finish(TransactionRequest<Object> sentinel) {
        if (isFinished.compareAndSet(false, true)) {
            try {
                channel.put(sentinel);
            } catch (InterruptedException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }
}
