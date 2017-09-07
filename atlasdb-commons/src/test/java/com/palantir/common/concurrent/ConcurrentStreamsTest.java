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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ConcurrentStreamsTest {

    private final ExecutorService executor = Executors.newFixedThreadPool(32);

    private static class CustomRuntimeException extends RuntimeException {}

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
    public void testShouldOnlyRunWithProvidedConcurrency() throws Exception {
        AtomicInteger numStarted = new AtomicInteger(0);
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    numStarted.getAndIncrement();
                    pause(100);
                    return value + 1;
                },
                executor,
                2);
        pause(50);
        Assert.assertEquals(numStarted.get(), 2);
        pause(100);
        Assert.assertEquals(numStarted.get(), 4);
        Assert.assertEquals(values.collect(Collectors.toList()), ImmutableList.of(2, 3, 4, 5));
    }

    @Test(expected = CustomRuntimeException.class)
    public void testShouldPropogateExceptions() {
        Stream<Integer> values = ConcurrentStreams.map(
                ImmutableList.of(1, 2, 3, 4),
                value -> {
                    throw new CustomRuntimeException();
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
                    throw new CustomRuntimeException();
                },
                executor,
                2);
    }

    private void pause(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
