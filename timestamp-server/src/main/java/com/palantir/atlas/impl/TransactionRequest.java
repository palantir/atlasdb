package com.palantir.atlas.impl;

import java.util.concurrent.SynchronousQueue;

import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;

class TransactionRequest<T> {
    private final RuntimeTransactionTask<T> task;
    private SynchronousQueue<T> channel = new SynchronousQueue<T>();

    public TransactionRequest(RuntimeTransactionTask<T> task) {
        this.task = task;
    }

    public RuntimeTransactionTask<T> getTask() {
        return task;
    }

    public void send(T item) throws InterruptedException {
        channel.put(item);
    }

    public T receive() throws InterruptedException {
        return channel.take();
    }
}
