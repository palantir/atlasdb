package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.base.ClosableIterator;

/**
 * {@link FutureTask} to be used in conjunction with {@link LazyClosableIterator}.
 * Allows for resulting iterators to be properly closed in the case of interrupts.
 *
 * @author dxiao
 */
public class FutureClosableIteratorTask<T> extends FutureTask<ClosableIterator<T>> {
    private static final Logger log = LoggerFactory.getLogger(FutureClosableIteratorTask.class);

    private volatile boolean canceled;
    private volatile ClosableIterator<T> outcome;

    public FutureClosableIteratorTask(Callable<ClosableIterator<T>> callable) {
        super(callable);
        this.canceled = false;
        this.outcome = null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        canceled = true;
        if (outcome != null) {
            close();
        }
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    protected void set(ClosableIterator<T> iterator) {
        super.set(iterator);
        outcome = iterator;
        if (canceled || Thread.currentThread().isInterrupted()) {
            close();
        }
    }

    private void close() {
        log.warn("Query was interrupted before a closable iterator was returned. " +
                // and query-er did the responsible thing and interrupted us.
                "closing resoures to prevent leaks.");
        try {
            outcome.close();
        } catch (RuntimeException e) {
            log.error("Error while trying to close query resources after interrupt.", e);
            throw e;
        }
    }
}