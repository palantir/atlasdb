/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AsyncRetrierTest {
    private static final int MAX_ATTEMPTS = 3;
    private static final Duration RETRY_PERIOD = Duration.ofSeconds(1);
    private static final Duration DELTA = Duration.ofMillis(1);

    private final DeterministicScheduler scheduler = new DeterministicScheduler();

    private SettableFuture<Integer> future = SettableFuture.create();
    private Supplier<ListenableFuture<Integer>> replaceableFunction = () -> future;

    @Mock
    private Supplier<ListenableFuture<Integer>> function;

    private AsyncRetrier<Integer> retrier;

    @Before
    public void before() {
        retrier = new AsyncRetrier<>(
                MAX_ATTEMPTS,
                RETRY_PERIOD,
                MoreExecutors.listeningDecorator(scheduler),
                MoreExecutors.listeningDecorator(scheduler),
                new Predicate<Integer>() {
                    private int seen = 0;

                    @Override
                    public boolean test(Integer integer) {
                        return seen++ == integer;
                    }
                });
        when(function.get()).thenAnswer(_inv -> replaceableFunction.get());
    }

    @Test
    public void failsFutureIfSupplierThrows() {
        RuntimeException exception = new RuntimeException();
        replaceableFunction = () -> {
            throw exception;
        };
        ListenableFuture<Integer> hopefullyFailedFuture = retrier.execute(function);
        scheduler.runUntilIdle();
        assertThatThrownBy(() -> Futures.getDone(hopefullyFailedFuture)).hasCause(exception);
    }

    @Test
    public void returnsImmediatelyIfPredicatePasses() throws ExecutionException {
        ListenableFuture<Integer> result = retrier.execute(function);
        future.set(0);
        scheduler.runUntilIdle();
        assertThat(Futures.getDone(result)).isZero();
    }

    @Test
    public void retriesIfPredicateFails() throws ExecutionException {
        ListenableFuture<Integer> result = retrier.execute(function);
        future.set(-1);
        scheduler.runUntilIdle();
        verify(function, times(1)).get();
        tick(RETRY_PERIOD.minus(DELTA));
        verify(function, times(1)).get();
        assertThat(result).isNotDone();
        tick(DELTA);
        verify(function, times(2)).get();
        assertThat(result).isNotDone();
        tick(RETRY_PERIOD.minus(DELTA));
        verify(function, times(2)).get();
        future = SettableFuture.create();
        future.set(2);
        tick(DELTA);
        assertThat(Futures.getDone(result)).isEqualTo(2);
    }

    @Test
    public void doesNotRetryIndefinitely() throws ExecutionException {
        ListenableFuture<Integer> result = retrier.execute(function);
        future.set(-1);
        tick(RETRY_PERIOD.multipliedBy(MAX_ATTEMPTS));
        assertThat(Futures.getDone(result)).isEqualTo(-1);
    }

    private void tick(Duration duration) {
        scheduler.tick(duration.toMillis(), TimeUnit.MILLISECONDS);
    }
}
