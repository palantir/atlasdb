package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.base.Throwables;

/**
 * To avoid race conditions in the case of thread interruption, it is recommended that you use
 * {@link FutureClosableIteratorTask} for this class.
 */
public class LazyClosableIterator<T> extends AbstractIterator<T> implements ClosableIterator<T> {
    private static final Logger log = LoggerFactory.getLogger(LazyClosableIterator.class);
    private final Queue<Future<ClosableIterator<T>>> futures;
    private ClosableIterator<T> currentIter = null;

    public LazyClosableIterator(Queue<Future<ClosableIterator<T>>> futures) {
        this.futures = futures;
    }

    @Override
    protected T computeNext() {
        while (true) {
            if (currentIter == null) {
                if (futures.isEmpty()) {
                    return endOfData();
                }
                currentIter = removeNext();
            }
            if (currentIter.hasNext()) {
                return currentIter.next();
            }
            currentIter.close();
            currentIter = null;
        }
    }

    @Override
    public void close() {
        if (currentIter != null) {
            currentIter.close();
        }
        while (!futures.isEmpty()) {
            futures.peek().cancel(true);
            removeNext().close();
        }
    }

    private ClosableIterator<T> removeNext() {
        Future<ClosableIterator<T>> future = futures.remove();
        try {
            return future.get();
        } catch (CancellationException e) {
            log.debug("Operation was cancelled.", e);
            return ClosableIterators.wrap(ImmutableList.<T>of().iterator());
        } catch (InterruptedException e) {
            // Interruption may have happened after resources were created, try to alert task
            // of interruption and hope resources are properly closed
            future.cancel(true);
            log.error("Operation was interrupted.", e);
            throw Throwables.rewrapAndThrowUncheckedException(e);
        } catch (ExecutionException e) {
            log.error("Operation failed.", e.getCause());
            throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
        }
    }
}