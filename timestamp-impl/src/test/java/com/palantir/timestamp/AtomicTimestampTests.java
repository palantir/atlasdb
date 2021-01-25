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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class AtomicTimestampTests {

    private static final long INITIAL_TIMESTAMP = 1_000;
    private final AtomicTimestamp timestamp = new AtomicTimestamp(INITIAL_TIMESTAMP);
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    @After
    public void after() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void shouldIncreaseTheValueToAHigherNumber() {
        TimestampRange range = timestamp.incrementBy(10);

        assertThat(range.getUpperBound()).isEqualTo(INITIAL_TIMESTAMP + 10);
    }

    @Test
    public void cannotOverflow() {
        timestamp.increaseTo(Long.MAX_VALUE);
        assertThatExceptionOfType(ArithmeticException.class).isThrownBy(() -> timestamp.incrementBy(1));
    }

    @Test
    public void handleConcurrentlyIncreasingTheValue() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> timestamp.incrementBy(1));
        }

        waitForExecutorToFinish();

        assertThat(timestamp.incrementBy(1).getUpperBound()).isEqualTo(INITIAL_TIMESTAMP + 101);
    }

    private void waitForExecutorToFinish() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
