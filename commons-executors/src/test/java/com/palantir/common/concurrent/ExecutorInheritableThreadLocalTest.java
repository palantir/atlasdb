/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.common.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ExecutorInheritableThreadLocalTest {
    private static final String orig = "Yo";
    private static List<Integer> outputList = Lists.newLinkedList();
    private final ExecutorService exec = PTExecutors.newCachedThreadPool();
    private static final ExecutorInheritableThreadLocal<String> local = new ExecutorInheritableThreadLocal<String>() {
            @Override
            protected String initialValue() {
                return orig;
            }
        };

    private static final ExecutorInheritableThreadLocal<Integer> localInt
            = new ExecutorInheritableThreadLocal<Integer>() {
                @Override
                protected Integer initialValue() {
                    return 1;
                }

                @Override
                protected Integer childValue(Integer parentValue) {
                    return parentValue + 1;
                }

                @Override
                protected Integer installOnChildThread(Integer childValue) {
                    outputList.clear();
                    outputList.add(childValue);
                    return childValue + 1;
                }

                @Override
                protected void uninstallOnChildThread() {
                    // We don't add to count here because the future will return before it is complete.
                    // outputList.add(get() + 1);
                }
            };

    private static final ExecutorInheritableThreadLocal<AtomicInteger> nullCallCount
            = new ExecutorInheritableThreadLocal<AtomicInteger>() {
                @Override
                protected AtomicInteger initialValue() {
                    return new AtomicInteger(0);
                }
            };
    private static final ExecutorInheritableThreadLocal<Integer> nullInts
            = new ExecutorInheritableThreadLocal<Integer>() {
                @Override
                protected Integer initialValue() {
                    nullCallCount.get().incrementAndGet();
                    return null;
                }

                @Override
                protected Integer childValue(Integer parentValue) {
                    Preconditions.checkArgument(parentValue == null);
                    nullCallCount.get().incrementAndGet();
                    return null;
                }

                @Override
                protected Integer installOnChildThread(Integer childValue) {
                    Preconditions.checkArgument(childValue == null);
                    nullCallCount.get().incrementAndGet();
                    return null;
                }

                @Override
                protected void uninstallOnChildThread() {
                    Preconditions.checkArgument(get() == null);

                    // We don't add to count here because the future will return before it is complete.
                    // nullCallCount.get().incrementAndGet();
                }
            };

    @After
    public void tearDown() throws Exception {
        exec.shutdownNow();
        local.remove();
        localInt.remove();
    }

    @Test
    public void testNullable() {
        local.set(null);
        assertNull(local.get());
    }

    @Test
    public void testSameThread() {
        local.set("whatup");
        ListeningExecutorService sameThreadExecutor = MoreExecutors.newDirectExecutorService();
        sameThreadExecutor.submit(PTExecutors.wrap(Callables.returning(null)));
        Assert.assertEquals("whatup", local.get());
    }

    @Test
    public void testRemove() {
        local.get();
        local.remove();
        assertEquals(orig, local.get());
    }

    @Test
    public void testCreate() {
        assertEquals(orig, local.get());
    }

    @Test
    public void testThread() throws InterruptedException, ExecutionException {
        String str = "whatup";
        local.set(str);
        Future<String> future = exec.submit(local::get);
        assertEquals(str, future.get());
    }

    @Test
    public void testChild() throws InterruptedException, ExecutionException {
        localInt.set(10);
        Future<?> future = exec.submit((Callable<Void>) () -> {
            outputList.add(localInt.get());
            return null;
        });
        future.get();
        assertEquals(ImmutableList.of(11, 12), outputList);
    }

    @Test
    public void testAllNulls() throws InterruptedException, ExecutionException {
        nullInts.remove();
        nullCallCount.set(new AtomicInteger(0));
        Preconditions.checkArgument(nullInts.get() == null);
        assertEquals(1, nullCallCount.get().get());
        Future<?> future = exec.submit((Callable<Void>) () -> {
            assertEquals(3, nullCallCount.get().get());
            Preconditions.checkArgument(nullInts.get() == null);
            assertEquals(3, nullCallCount.get().get());
            return null;
        });
        future.get();
        assertEquals(3, nullCallCount.get().get());
        Preconditions.checkArgument(nullInts.get() == null);
        assertEquals(3, nullCallCount.get().get());
    }
}
