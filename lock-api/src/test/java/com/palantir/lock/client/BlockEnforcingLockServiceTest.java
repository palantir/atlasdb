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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Streams;
import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked") // Mocks with generic types
public class BlockEnforcingLockServiceTest {
    private static final Exception TIMEOUT_EXCEPTION = new RuntimeException(new SocketTimeoutException("timeout"));
    private static final Exception RUNTIME_EXCEPTION = new RuntimeException();

    private final Clock clock = mock(Clock.class);
    private final BlockEnforcingLockService.RemoteTimeoutRetryer retryer =
            new BlockEnforcingLockService.RemoteTimeoutRetryer(clock);
    private final Function<Duration, Outcome> query = mock(Function.class);

    @Test
    public void returnsRequestImmediatelyIfSuccessful() {
        whenMakingRequestsReceive(OutcomeAndInstant.successAfter(4));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.SUCCESS);

        verify(query).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void returnsTimeoutResponseIfFailed() {
        whenMakingRequestsReceive(OutcomeAndInstant.explicitTimeoutAfter(50));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.EXPLICIT_TIMEOUT);

        verify(query).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void returnsTimeoutResponseAfterTryingMultipleTimes() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.explicitTimeoutAfter(5),
                OutcomeAndInstant.explicitTimeoutAfter(7),
                OutcomeAndInstant.explicitTimeoutAfter(9),
                OutcomeAndInstant.explicitTimeoutAfter(13));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.EXPLICIT_TIMEOUT);

        verify(query, times(4)).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void restrictsRequestsToCorrectTimeBounds() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.explicitTimeoutAfter(5),
                OutcomeAndInstant.explicitTimeoutAfter(7),
                OutcomeAndInstant.explicitTimeoutAfter(9),
                OutcomeAndInstant.explicitTimeoutAfter(13));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.EXPLICIT_TIMEOUT);

        ArgumentCaptor<Duration> captor = ArgumentCaptor.forClass(Duration.class);
        verify(query, times(4)).apply(captor.capture());
        assertThat(captor.getAllValues())
                .containsExactly(
                        Duration.ofMillis(10),
                        Duration.ofMillis(10 - 5),
                        Duration.ofMillis(10 - 7),
                        Duration.ofMillis(10 - 9));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void recoversFromExplicitTimeouts() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.explicitTimeoutAfter(5),
                OutcomeAndInstant.explicitTimeoutAfter(7),
                OutcomeAndInstant.explicitTimeoutAfter(9),
                OutcomeAndInstant.successAfter(13));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.SUCCESS);

        verify(query, times(4)).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void propagatesTimeoutExceptionIfPastDeadline() {
        whenMakingRequestsReceive(OutcomeAndInstant.timeoutByExceptionAfter(50));

        assertThatThrownBy(this::retryForTenMillisUntilTrue).isEqualTo(TIMEOUT_EXCEPTION);

        verify(query).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void recoversFromTimeoutExceptions() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.timeoutByExceptionAfter(5),
                OutcomeAndInstant.timeoutByExceptionAfter(7),
                OutcomeAndInstant.timeoutByExceptionAfter(9),
                OutcomeAndInstant.successAfter(13));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.SUCCESS);

        verify(query, times(4)).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void recoversFromMixtureOfExplicitAndExceptionBasedTimeouts() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.explicitTimeoutAfter(3),
                OutcomeAndInstant.timeoutByExceptionAfter(5),
                OutcomeAndInstant.explicitTimeoutAfter(7),
                OutcomeAndInstant.timeoutByExceptionAfter(9),
                OutcomeAndInstant.successAfter(13));

        assertThat(retryForTenMillisUntilTrue()).isEqualTo(Outcome.SUCCESS);

        verify(query, times(5)).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void propagatesRuntimeExceptionImmediately() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.nonRecoverableExceptionAfter(6), OutcomeAndInstant.nonRecoverableExceptionAfter(11));

        assertThatThrownBy(this::retryForTenMillisUntilTrue).isEqualTo(RUNTIME_EXCEPTION);

        verify(query).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void propagatesRuntimeExceptionEvenIfDurationExpired() {
        whenMakingRequestsReceive(
                OutcomeAndInstant.explicitTimeoutAfter(6), OutcomeAndInstant.nonRecoverableExceptionAfter(25));

        assertThatThrownBy(this::retryForTenMillisUntilTrue).isEqualTo(RUNTIME_EXCEPTION);

        verify(query, times(2)).apply(any(Duration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void requestWithZeroTimeoutIsStillAttemptedAtLeastOnce() {
        whenMakingRequestsReceive(OutcomeAndInstant.successAfter(0));

        Outcome outcome = retryForDurationUntilTrue(Duration.ZERO);
        assertThat(outcome).isEqualTo(Outcome.SUCCESS);

        verify(query).apply(any(Duration.class));
    }

    private Outcome retryForDurationUntilTrue(Duration duration) {
        return retryer.attemptUntilTimeLimitOrException(
                duration,
                duration,
                (fst, snd) -> fst.compareTo(snd) < 0 ? fst : snd,
                query,
                outcome -> outcome == Outcome.EXPLICIT_TIMEOUT);
    }

    private Outcome retryForTenMillisUntilTrue() {
        return retryForDurationUntilTrue(Duration.ofMillis(10));
    }

    private void whenMakingRequestsReceive(OutcomeAndInstant... results) {
        List<OutcomeAndInstant> outcomesAndInstants = Arrays.stream(results).collect(Collectors.toList());
        AtomicInteger positionalIndex = new AtomicInteger(0);

        List<Instant> clockInstants = Streams.concat(
                        Stream.of(Instant.EPOCH), outcomesAndInstants.stream().map(OutcomeAndInstant::instant))
                .collect(Collectors.toList());

        when(clock.instant())
                .thenAnswer(invocation -> clockInstants.get(Math.min(clockInstants.size() - 1, positionalIndex.get())));
        when(query.apply(any())).thenAnswer(invocation -> {
            try {
                Outcome outcome = outcomesAndInstants
                        .get(Math.min(outcomesAndInstants.size() - 1, positionalIndex.get()))
                        .outcome();
                switch (outcome) {
                    case SUCCESS:
                    case EXPLICIT_TIMEOUT:
                        return outcome;
                    case TIMEOUT_BY_EXCEPTION:
                        throw TIMEOUT_EXCEPTION;
                    case NON_RECOVERABLE_EXCEPTION:
                        throw RUNTIME_EXCEPTION;
                    default:
                        throw new AssertionError("Unknown case in Outcome: " + outcome);
                }
            } finally {
                positionalIndex.getAndIncrement();
            }
        });
    }

    enum Outcome {
        SUCCESS,
        EXPLICIT_TIMEOUT,
        TIMEOUT_BY_EXCEPTION,
        NON_RECOVERABLE_EXCEPTION
    }

    @Value.Immutable
    interface OutcomeAndInstant {
        @Value.Parameter
        Outcome outcome();

        @Value.Parameter
        Instant instant();

        static OutcomeAndInstant successAfter(int millis) {
            return ImmutableOutcomeAndInstant.of(Outcome.SUCCESS, Instant.ofEpochMilli(millis));
        }

        static OutcomeAndInstant explicitTimeoutAfter(int millis) {
            return ImmutableOutcomeAndInstant.of(Outcome.EXPLICIT_TIMEOUT, Instant.ofEpochMilli(millis));
        }

        static OutcomeAndInstant timeoutByExceptionAfter(int millis) {
            return ImmutableOutcomeAndInstant.of(Outcome.TIMEOUT_BY_EXCEPTION, Instant.ofEpochMilli(millis));
        }

        static OutcomeAndInstant nonRecoverableExceptionAfter(int millis) {
            return ImmutableOutcomeAndInstant.of(Outcome.NON_RECOVERABLE_EXCEPTION, Instant.ofEpochMilli(millis));
        }
    }
}
