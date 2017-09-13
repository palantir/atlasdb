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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ConcurrentStreamsTest {

    private final ExecutorService executor = Executors.newFixedThreadPool(32);

    private static class CustomExecutionException extends RuntimeException {}
    private static class LatchTimedOutException extends RuntimeException {}

    @Test
    public void testDoesEvaluateResultsWithFullConcurrency() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4, 5, 6, 7),
                value -> value + 1,
                executor,
                7);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5, 6, 7, 8));
    }

    @Test
    public void testDoesEvaluateResultsWhenLimitingConcurrency() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                value -> value + 1,
                executor,
                2);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11));
    }

    @Test
    public void testDoesMaintainCorrectOrdering() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(5, 3, 6, 2, 1, 9),
                value -> {
                    if (value <= 3) {
                        // Add delay on some elements so that subsequent values will be processed first so that
                        // we can better guarantee the ordering is maintained regardless of processing order
                        pause(100);
                    }
                    return value + 1;
                },
                executor,
                2);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(6, 4, 7, 3, 2, 10));
    }

    @Test
    public void testCanHandleValueDuplicates() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 1, 2),
                value -> value + 1,
                executor,
                2);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 2, 3));
    }

    @Test
    public void testShouldOnlyRunWithProvidedConcurrency() throws Exception {
        CountDownLatch firstTwoValuesStartProcessing = new CountDownLatch(3);
        CountDownLatch mainThreadHasValidatedFirstTwoValues = new CountDownLatch(3);
        CountDownLatch latterTwoValuesStartProcessing = new CountDownLatch(3);

        AtomicInteger numStarted = new AtomicInteger(0);
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    numStarted.getAndIncrement();
                    if (value <= 2) {
                        countdownAndBlock(firstTwoValuesStartProcessing);
                        countdownAndBlock(mainThreadHasValidatedFirstTwoValues);
                    } else {
                        countdownAndBlock(latterTwoValuesStartProcessing);
                    }
                    return value + 1;
                },
                executor,
                2);

        countdownAndBlock(firstTwoValuesStartProcessing);
        Assert.assertEquals(numStarted.get(), 2);
        countdownAndBlock(mainThreadHasValidatedFirstTwoValues);
        countdownAndBlock(latterTwoValuesStartProcessing);
        Assert.assertEquals(numStarted.get(), 4);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5));
    }

    @Test(expected = CustomExecutionException.class)
    public void testShouldPropogateExceptions() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    throw new CustomExecutionException();
                },
                executor,
                2);
        values.collect(Collectors.toList());
    }

    @Test
    public void testShouldNotThrowBeforeCollected() {
        ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    throw new CustomExecutionException();
                },
                executor,
                2);
    }

    @Test
    public void testShouldAbortWaitingTasksEarlyOnFailure() {
        CountDownLatch firstTwoValuesIncrementCounter = new CountDownLatch(3);
        AtomicInteger numStarted = new AtomicInteger(0);

        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    numStarted.getAndIncrement();
                    countdownAndBlock(firstTwoValuesIncrementCounter);
                    throw new CustomExecutionException();
                },
                executor,
                2);

        countdownAndBlock(firstTwoValuesIncrementCounter);
        try {
            values.collect(Collectors.toList());
        } catch (CustomExecutionException e) {
            Assert.assertEquals(numStarted.get(), 2);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCanOperateOnStreamWhileTasksAreStillRunning() {
        CountDownLatch latterTwoValuesStartProcessing = new CountDownLatch(3);
        CountDownLatch latterTwoValuesIncrementCounter = new CountDownLatch(3);

        AtomicInteger numStarted = new AtomicInteger(0);
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    if (value < 3) {
                        numStarted.getAndIncrement();
                    } else {
                        countdownAndBlock(latterTwoValuesStartProcessing);
                        numStarted.getAndIncrement();
                        countdownAndBlock(latterTwoValuesIncrementCounter);
                    }
                    return value + 1;
                },
                executor,
                2);
        values.forEach(value -> {
            if (value <= 3) {
                Assert.assertEquals(numStarted.get(), 2);
                if (value == 3) {
                    // Release last two threads and then ensure they both increment the counter before proceeding
                    countdownAndBlock(latterTwoValuesStartProcessing);
                    countdownAndBlock(latterTwoValuesIncrementCounter);
                }
            } else {
                Assert.assertEquals(numStarted.get(), 4);
            }
        });
    }

    private void countdownAndBlock(CountDownLatch latch) {
        latch.countDown();
        try {
            if (!latch.await(1, TimeUnit.SECONDS)) {
                throw new LatchTimedOutException();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void pause(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}