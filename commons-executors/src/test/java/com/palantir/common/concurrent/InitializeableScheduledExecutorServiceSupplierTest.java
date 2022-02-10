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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class InitializeableScheduledExecutorServiceSupplierTest {
    private final InitializeableScheduledExecutorServiceSupplier supplier =
            new InitializeableScheduledExecutorServiceSupplier(new NamedThreadFactory("test", true));

    @Test
    public void uninitializedSupplierThrows() {
        assertThatThrownBy(supplier::get).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canInitializeSupplier() throws ExecutionException, InterruptedException {
        supplier.initialize(1);

        assertThat(supplier.get().schedule(() -> 1L, 0, TimeUnit.SECONDS).get()).isEqualTo(1L);
        supplier.get().shutdownNow();
    }

    @Test
    public void originalNumberOfThreadsIsRespected() throws InterruptedException {
        InitializeableScheduledExecutorServiceSupplier supplier =
                new InitializeableScheduledExecutorServiceSupplier(new NamedThreadFactory("test", true));
        supplier.initialize(1);
        supplier.initialize(2);

        CountDownLatch latch = new CountDownLatch(2);
        Runnable task = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                // ok cool
            }
        };

        supplier.get().schedule(task, 0, TimeUnit.SECONDS);
        supplier.get().schedule(task, 0, TimeUnit.SECONDS);
        assertThat(latch.await(1, TimeUnit.SECONDS)).isFalse();
    }
}
