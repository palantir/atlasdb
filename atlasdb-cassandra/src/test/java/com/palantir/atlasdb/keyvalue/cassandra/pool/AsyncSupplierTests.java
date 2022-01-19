/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.concurrent.PTExecutors;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.Test;

public final class AsyncSupplierTests {
    @Test
    public void supplierMemoizesResult() {
        AtomicInteger calls = new AtomicInteger(0);
        AsyncSupplier<Integer> supplier = AsyncSupplier.create(() -> Optional.of(calls.incrementAndGet()));

        Awaitility.await("wait for computation to complete")
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(supplier.get()).hasValue(1));

        assertThat(supplier.get()).hasValue(1);
    }

    @Test
    public void supplierDoesNotBlockIfDelegateBlocks() {
        CountDownLatch taskExecuting = new CountDownLatch(1);
        CountDownLatch begunExecution = new CountDownLatch(1);
        AsyncSupplier<Integer> supplier = AsyncSupplier.create(() -> {
            begunExecution.countDown();
            Uninterruptibles.awaitUninterruptibly(taskExecuting);
            return Optional.of(1);
        });

        assertThat(supplier.get()).isEmpty();
        Uninterruptibles.awaitUninterruptibly(begunExecution);
        assertThat(supplier.get()).isEmpty();

        taskExecuting.countDown();
        Awaitility.await("wait for computation to complete")
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(supplier.get()).hasValue(1));
    }

    @Test
    public void throwingSupplierOnlyThrowsOnceAndReturnsEmptyForeverMore() {
        AtomicInteger calls = new AtomicInteger(0);
        CountDownLatch taskDone = new CountDownLatch(1);
        AsyncSupplier<Integer> supplier = AsyncSupplier.create(() -> {
            calls.incrementAndGet();
            try {
                throw new RuntimeException();
            } finally {
                taskDone.countDown();
            }
        });

        assertThat(supplier.get()).isEmpty();
        Uninterruptibles.awaitUninterruptibly(taskDone);

        for (int attempt = 0; attempt < 100; attempt++) {
            assertThat(supplier.get()).isEmpty();
        }

        assertThat(calls).hasValue(1);
    }

    @Test
    public void executorIsShutdownAfterExecution() {
        ExecutorService executor = PTExecutors.newSingleThreadExecutor();
        AsyncSupplier<Integer> supplier = new AsyncSupplier<>(() -> Optional.of(1), executor);

        Awaitility.await("wait for computation to complete")
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(supplier.get()).hasValue(1));
        assertThat(executor.isShutdown()).isTrue();
    }
}
