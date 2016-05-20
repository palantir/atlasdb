/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
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
                currentIter = getClosableIterator(futures.remove());
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
        try (Closer closer = Closer.create()) {
            if (currentIter != null) {
                closer.register(currentIter);
            }
            while (!futures.isEmpty()) {
                try {
                    futures.peek().cancel(true);
                } finally {
                    Future<ClosableIterator<T>> future = futures.remove();
                    Closeable closeable = new Closeable() {
                        @Override
                        public void close() throws IOException {
                            getClosableIterator(future).close();
                        }
                    };
                    closer.register(closeable);
                }
            }
        } catch (IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private ClosableIterator<T> getClosableIterator(Future<ClosableIterator<T>> future) {
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