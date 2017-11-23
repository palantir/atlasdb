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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;

public class QosCassandraWriteEteTest extends QosCassandraEteTestSetup {

    @Test
    public void shouldBeAbleToWriteSmallAmountOfBytesIfDoesNotExceedLimit() {
        writeNTodosOfSize(1, 100);
    }

    @Test
    public void shouldBeAbleToWriteSmallAmountOfBytesSeriallyIfDoesNotExceedLimit() {
        IntStream.range(0, 50).forEach(i -> writeNTodosOfSize(1, 100));
    }

    @Test
    public void shouldBeAbleToWriteLargeAmountsExceedingTheLimitFirstTime() {
        writeNTodosOfSize(12, 1_000);
    }

    @Test
    public void shouldBeAbleToWriteLargeAmountsExceedingTheLimitSecondTimeWithSoftLimiting() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        writeNTodosOfSize(1, 20_000);
        long firstWriteTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        stopwatch = Stopwatch.createStarted();
        writeNTodosOfSize(200, 1_000);
        long secondWriteTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        assertThat(secondWriteTime).isGreaterThan(firstWriteTime);
        assertThat(secondWriteTime - firstWriteTime).isLessThan(MAX_SOFT_LIMITING_SLEEP_MILLIS);
    }

    @Test
    public void shouldNotBeAbleToWriteLargeAmountsIfSoftLimitSleepWillBeMoreThanConfiguredBackoffTime() {
        // Have one quick limit-exceeding write, as the rate-limiter
        // will let anything pass through until the limit is exceeded.
        writeNTodosOfSize(1, 100_000);

        assertThatThrownBy(() -> writeNTodosOfSize(1, 100_000))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessage("Rate limited. Available capacity has been exhausted.");

        // One write smaller than the rate limit should also be rate limited.
        assertThatThrownBy(() -> writeNTodosOfSize(5, 10))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessage("Rate limited. Available capacity has been exhausted.");
    }

    @Test
    public void writeRateLimitShouldBeRespectedByConcurrentWritingThreads() throws InterruptedException {
        int oneTodoSizeInBytes = 167;

        int numThreads = 5;
        int numWritesPerThread = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future> futures = new ArrayList<>(numThreads);

        long start = System.nanoTime();
        IntStream.range(0, numThreads).forEach(i ->
                futures.add(executorService.submit(() -> {
                    IntStream.range(0, numWritesPerThread).forEach(j -> writeNTodosOfSize(1, 100));
                    return null;
                })));
        executorService.shutdown();
        Preconditions.checkState(executorService.awaitTermination(30L, TimeUnit.SECONDS),
                "Read tasks did not finish in 30s");
        long writeTime = System.nanoTime() - start;

        assertThatAllWritesWereSuccessful(futures);
        double actualBytesWritten = numThreads * numWritesPerThread * oneTodoSizeInBytes;
        double maxReadBytesLimit = readBytesPerSecond * ((double) writeTime / TimeUnit.SECONDS.toNanos(1)
                + 5 /* to allow for rate-limiter burst */);
        assertThat(actualBytesWritten).isLessThan(maxReadBytesLimit);
    }

    private void assertThatAllWritesWereSuccessful(List<Future> futures) {
        AtomicInteger exceptionCounter = new AtomicInteger(0);
        futures.forEach(future -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RateLimitExceededException) {
                    exceptionCounter.getAndIncrement();
                }
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        });
        assertThat(exceptionCounter.get()).isEqualTo(0);
    }
}
