// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.common.concurrent;


import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class ExecutorInheritableThreadLocalTest extends Assert {
    private static final String orig = "Yo";
    private static List<Integer> outputList = Lists.newLinkedList();
    private final ExecutorService exec = PTExecutors.newCachedThreadPool();
    private static final ExecutorInheritableThreadLocal<String> local = new ExecutorInheritableThreadLocal<String>() {
            @Override
            protected String initialValue() {
                return orig;
            }
        };

    private static final ExecutorInheritableThreadLocal<Integer> localInt = new ExecutorInheritableThreadLocal<Integer>() {
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
                outputList.add(get() + 1);
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
        ListeningExecutorService sameThreadExecutor = MoreExecutors.sameThreadExecutor();
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
        String s = "whatup";
        local.set(s);
        Future<String> f = exec.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return local.get();
            }
        });
        assertEquals(s, f.get());
    }

    @Test
    public void testChild() throws InterruptedException, ExecutionException {
        localInt.set(10);
        Future<?> future = exec.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                outputList.add(localInt.get());
                return null;
            }
        });
        future.get();
        Thread.sleep(10);
        assertEquals(ImmutableList.of(11, 12, 13), outputList);
    }
}
