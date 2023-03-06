/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import com.google.common.util.concurrent.Futures;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ThreadTestUtils {

    private ThreadTestUtils() {}

    public static void runTaskOnMultipleThreads(Runnable runnable, int numThreads) throws InterruptedException {
        CountDownLatch startTaskLatch = new CountDownLatch(1);
        ExecutorService taskExecutor = Executors.newCachedThreadPool();

        List<Future<Void>> futures = IntStream.range(0, numThreads)
                .mapToObj(index -> taskExecutor.submit((Callable<Void>) () -> {
                    try {
                        startTaskLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    runnable.run();
                    return null;
                }))
                .collect(Collectors.toList());

        startTaskLatch.countDown();
        taskExecutor.shutdown();
        taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
        futures.forEach(Futures::getUnchecked);
    }
}
