/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntPredicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.github.rholder.retry.BlockStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.transaction.api.TransactionFailedNonRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.impl.TransactionRetryStrategy.Retryable;

@RunWith(MockitoJUnitRunner.class)
public class TransactionRetryStrategyTest {
    private final TestBlockStrategy blockStrategy = new TestBlockStrategy();
    @Mock private ThreadLocalRandom random;
    @Mock private Retryable<String, Exception> task;
    @Mock private IntPredicate shouldStopRetrying;

    private TransactionRetryStrategy legacy;
    private TransactionRetryStrategy exponential;

    private static final class TestBlockStrategy implements BlockStrategy {
        private long totalBlockedTime;

        @Override
        public void block(long sleepTime) {
            totalBlockedTime += sleepTime;
        }
    }

    @Before
    public void before() {
        when(random.nextLong(anyLong())).thenAnswer(inv -> (long) inv.getArgument(0) - 1);
        when(shouldStopRetrying.test(anyInt())).thenAnswer(inv -> ((int) inv.getArgument(0)) > 1);
        legacy = TransactionRetryStrategy.createLegacy(blockStrategy);
        exponential = TransactionRetryStrategy.createExponential(blockStrategy, random);

    }

    private String run() throws Exception {
        return exponential.runWithRetry(shouldStopRetrying, task);
    }

    @Test
    public void retriesIfFailsWithRetriableException() throws Exception {
        when(task.run()).thenThrow(new TransactionFailedRetriableException("")).thenReturn("success");
        assertThat(run()).isEqualTo("success");
    }

    @Test
    public void stopsIfShouldStopRetrying() throws Exception {
        TransactionFailedRetriableException second = new TransactionFailedRetriableException("second");
        when(task.run()).thenThrow(new TransactionFailedRetriableException("first")).thenThrow(second);
        assertThatExceptionOfType(TransactionFailedRetriableException.class)
                .isThrownBy(this::run)
                .withMessage("Failing after 2 tries.")
                .withCause(second);
    }

    @Test
    public void doesNotRetryOnNonRetriableTransactionFailedException() throws Exception {
        TransactionFailedNonRetriableException failure = new TransactionFailedNonRetriableException("");
        when(task.run()).thenThrow(failure);
        assertThatThrownBy(this::run).isEqualTo(failure);
    }

    @Test
    public void rethrowsExceptions() throws Exception {
        Exception exception = new Exception("rethrown");
        when(task.run()).thenThrow(exception);
        assertThatThrownBy(this::run).isEqualTo(exception);
    }

    @Test
    public void rethrowsErrors() throws Exception {
        AssertionError error = new AssertionError();
        when(task.run()).thenThrow(error);
        assertThatThrownBy(this::run).isEqualTo(error);
    }

    @Test
    public void doesNotBackOffIfLegacy() throws Exception {
        when(shouldStopRetrying.test(anyInt())).thenAnswer(inv -> ((int) inv.getArgument(0)) > 50);
        when(task.run()).thenThrow(new TransactionFailedRetriableException(""));
        assertThatExceptionOfType(TransactionFailedRetriableException.class)
                .isThrownBy(() -> legacy.runWithRetry(shouldStopRetrying, task));
        assertThat(blockStrategy.totalBlockedTime).isZero();
    }

    @Test
    public void backsOffExponentially() throws Exception {
        when(shouldStopRetrying.test(anyInt())).thenAnswer(inv -> ((int) inv.getArgument(0)) > 11);
        when(task.run()).thenAnswer(inv -> {
            System.out.println(blockStrategy.totalBlockedTime);
            throw new TransactionFailedRetriableException("")
;        });
        List<Integer> rawBlockTimes = ImmutableList.of(
                200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 60000, 60000);
        List<Integer> randomizedBlockTimes = Lists.transform(rawBlockTimes, x -> x - 1);
        long expectedBlockDuration = randomizedBlockTimes.stream().mapToInt(x -> x).sum();
        assertThatExceptionOfType(TransactionFailedRetriableException.class).isThrownBy(this::run);
        assertThat(blockStrategy.totalBlockedTime).isEqualTo(expectedBlockDuration);
    }
}