/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class RequestLimitedExecutorServiceTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(10);
    private static final int MAX_CONCURRENCY = 2;
    private static final Duration TIMEOUT = Duration.ofMillis(100);

    private RequestLimitedExecutorService limitedExecutor;

    @Before
    public void before() {
        limitedExecutor = RequestLimitedExecutorService.fromDelegate(EXECUTOR, MAX_CONCURRENCY, TIMEOUT);
    }

    @Test
    public void testCanSubmitUpToMax() {
        AtomicInteger value1 = new AtomicInteger(1);
        AtomicInteger value2 = new AtomicInteger(2);

        finishAll(ImmutableList.of(
                runAndBlock(() -> value1.set(3)),
                runAndBlock(() -> value2.set(4))));

        Assert.assertEquals(value1.get(), 3);
        Assert.assertEquals(value2.get(), 4);
    }

    @Test(expected = RuntimeException.class)
    public void testWillNotRunOverMax() {
        finishAll(ImmutableList.of(
                runAndBlock(() -> { }),
                runAndBlock(() -> { }),
                runAndBlock(() -> { })));
    }

    @Test
    public void testCanGoOverMaxWhenPriorTestFinishes() {
        finishAll(ImmutableList.of(run(() -> { }), run(() -> { })));
        finishAll(ImmutableList.of(run(() -> { })));
    }

    private Future<?> run(Runnable runnable) {
        return limitedExecutor.submit(runnable::run);
    }

    private Future<?> runAndBlock(Runnable runnable) {
        return run(() -> {
            runnable.run();
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void finishAll(List<Future<?>> futures) {
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
