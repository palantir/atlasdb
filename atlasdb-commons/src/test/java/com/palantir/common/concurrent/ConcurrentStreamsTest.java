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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ConcurrentStreamsTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(1);

    private final ExecutorService executor = Executors.newFixedThreadPool(32);

    @Test
    public void testDoesEvaluateResultsWithFullConcurrency() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> value + 1,
                executor,
                4,
                TIMEOUT);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5));
    }

    @Test
    public void testDoesEvaluateResultsWhenLimitingConcurrency() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> value + 1,
                executor,
                2,
                TIMEOUT);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5));
    }

    @Test(expected = RuntimeException.class)
    public void testShouldOnlyRunWithProvidedConcurrency() {
        CountDownLatch latch = new CountDownLatch(3);
        ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    decrementAndWaitOnLatchForLongerThanExecutorTimeout(latch);
                    return value + 1;
                },
                executor,
                2,
                TIMEOUT);
    }

    private void decrementAndWaitOnLatchForLongerThanExecutorTimeout(CountDownLatch latch) {
        latch.countDown();
        try {
            latch.await(2 * TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
