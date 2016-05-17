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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.exception.PalantirInterruptedException;

public class LazyClosableIteratorTest {

    @Test
    public void testInterruptsDontCauseLeaksMultiple() throws Exception {
        for (int i = 1; i <= 100; i++) {
            try {
                // Because race conditions are hard...
                testInterruptsDontCauseLeaks();
            } catch (Throwable e) {
                throw new RuntimeException("Failure during run " + i, e);
            }
        }
    }

    @Test // QA-89231
    public void testInterruptsDontCauseLeaks() throws Exception {
        Set<Integer> opened = Sets.newConcurrentHashSet();
        Set<Integer> closed = Sets.newConcurrentHashSet();
        List<FutureTask<ClosableIterator<String>>> tasks = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            tasks.add(createNewFutureTask(i, opened, closed));
        }
        tasks.add(createInterruptTask(Thread.currentThread(), opened, closed));
        for (int i = 2; i < 4; i++) {
            tasks.add(createNewFutureTask(i, opened, closed));
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);
        Queue<Future<ClosableIterator<String>>> futures = Queues.newArrayDeque();
        futures.addAll(tasks);
        for (FutureTask<?> task : tasks) {
            exec.submit(task);
        }
        LazyClosableIterator<String> iter = new LazyClosableIterator<String>(futures);

        List<String> output = Lists.newArrayList();
        try {
            while(iter.hasNext()) {
                output.add(iter.next());
            }
            assertTrue(Thread.interrupted()); // race didn't do the thing this time.
        } catch (PalantirInterruptedException e) {
            assertTrue(Thread.interrupted()); // clear interrupted exception
        }

        iter.close();
        exec.shutdownNow();
        exec.awaitTermination(1, TimeUnit.SECONDS);
        assertEquals(opened, closed);
        assertTrue("output not in allowable state: " + output,
                output.equals(Lists.newArrayList("0-1", "0-2", "1-1", "1-2")) ||
                output.equals(Lists.newArrayList("0-1", "0-2")) ||
                output.isEmpty());
    }

    private FutureClosableIteratorTask<String> createInterruptTask(final Thread curThread,
            final Set<Integer> opened, final Set<Integer> closed) {
        return new FutureClosableIteratorTask<String>(new Callable<ClosableIterator<String>>() {
            @Override
            public ClosableIterator<String> call() {
                opened.add(-1);
                curThread.interrupt();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    closed.add(-1);
                }
                throw new RuntimeException();
            }
        });
    }

    private FutureClosableIteratorTask<String> createNewFutureTask(final int num,
            final Set<Integer> opened, final Set<Integer> closed) {
        return new FutureClosableIteratorTask<String>(new Callable<ClosableIterator<String>>() {
            @Override
            public ClosableIterator<String> call() throws Exception {
                final Iterator<String> iter = Iterators.forArray(num + "-1", num + "-2");
                opened.add(num);
                ClosableIterator<String> result = ClosableIterators.wrap(
                        new AbstractIterator<String>() {
                            @Override
                            protected String computeNext() {
                                if (iter.hasNext()) {
                                    return iter.next();
                                } else {
                                    return endOfData();
                                }
                            }
                        },
                        new Closeable() {
                            @Override
                            public void close() {
                                closed.add(num);
                            }
                        });
               return result;
            }
        });
    }
}
