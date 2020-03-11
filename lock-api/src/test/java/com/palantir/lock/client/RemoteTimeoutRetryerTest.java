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

import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import org.immutables.value.Value;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked") // Mocks with generic types
public class RemoteTimeoutRetryerTest {
    private static final int DEFAULT_VALUE = -1;
    private static final int QUERY_VALUE = 3;
    private static final int FORTY_TWO = 42;
    private static final Exception TIMEOUT_EXCEPTION = new RuntimeException(new SocketTimeoutException("timeout"));
    private static final Exception RUNTIME_EXCEPTION = new RuntimeException();

    private final Clock clock = mock(Clock.class);
    private final RemoteTimeoutRetryer retryer = new RemoteTimeoutRetryer(clock);
    private final Function<IntAndDuration, Integer> query = mock(Function.class);

    @Test
    public void returnsRequestImmediatelyIfSuccessful() {
        setupDefaultClock();
        when(query.apply(any())).thenReturn(FORTY_TWO);

        int result = retryForTenMillisAcceptingOnlyFortyTwo();
        assertThat(result).isEqualTo(FORTY_TWO);

        verify(query).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void returnsDefaultIfAlwaysFailing() {
        setupDefaultClock();
        when(query.apply(any())).thenReturn(5);

        int result = retryForTenMillisAcceptingOnlyFortyTwo();
        assertThat(result).isEqualTo(DEFAULT_VALUE);

        verify(query, times(3)).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void blockingDurationsDecreaseAsTimePasses() {
        setupDefaultClock();
        when(query.apply(any())).thenReturn(5);

        retryForTenMillisAcceptingOnlyFortyTwo();

        ArgumentCaptor<IntAndDuration> captor = ArgumentCaptor.forClass(IntAndDuration.class);
        verify(query, times(3)).apply(captor.capture());
        assertThat(captor.getAllValues()).containsExactly(
                ImmutableIntAndDuration.of(QUERY_VALUE, Duration.ofMillis(10)),
                ImmutableIntAndDuration.of(QUERY_VALUE, Duration.ofMillis(6)),
                ImmutableIntAndDuration.of(QUERY_VALUE, Duration.ofMillis(2)));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void recoversFromFailureIfWeBecomeSuccessful() {
        setupDefaultClock();
        when(query.apply(any()))
                .thenReturn(13)
                .thenReturn(FORTY_TWO);

        int result = retryForTenMillisAcceptingOnlyFortyTwo();
        assertThat(result).isEqualTo(FORTY_TWO);

        verify(query, times(2)).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void recoversFromRetryableExceptionIfWeBecomeSuccessful() {
        setupDefaultClock();
        when(query.apply(any()))
                .thenThrow(TIMEOUT_EXCEPTION)
                .thenReturn(FORTY_TWO);

        int result = retryForTenMillisAcceptingOnlyFortyTwo();
        assertThat(result).isEqualTo(FORTY_TWO);

        verify(query, times(2)).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void propagatesRetryableExceptionIfUnsuccessfulByDeadline() {
        setupDefaultClock();
        when(query.apply(any())).thenThrow(TIMEOUT_EXCEPTION);

        assertThatThrownBy(this::retryForTenMillisAcceptingOnlyFortyTwo).isEqualTo(TIMEOUT_EXCEPTION);
        // The following looks funky, but is correct: we call the clock one more time on a timeout exception.
        verify(query, times(2)).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    @Test
    public void throwsNonRetryableExceptionImmediately() {
        setupDefaultClock();
        when(query.apply(any())).thenThrow(RUNTIME_EXCEPTION);

        assertThatThrownBy(this::retryForTenMillisAcceptingOnlyFortyTwo).isEqualTo(RUNTIME_EXCEPTION);
        verify(query).apply(any(IntAndDuration.class));
        verifyNoMoreInteractions(query);
    }

    private Integer retryForTenMillisAcceptingOnlyFortyTwo() {
        return retryer.attemptUntilTimeLimitOrException(
                ImmutableIntAndDuration.of(QUERY_VALUE, Duration.ofMillis(10)),
                IntAndDuration::duration,
                (intAndDuration, newDuration) -> ImmutableIntAndDuration.of(intAndDuration.number(), newDuration),
                query,
                x -> x == FORTY_TWO,
                DEFAULT_VALUE);
    }

    private void setupDefaultClock() {
        when(clock.instant())
                .thenReturn(Instant.EPOCH)
                .thenReturn(Instant.EPOCH.plus(Duration.ofMillis(4)))
                .thenReturn(Instant.EPOCH.plus(Duration.ofMillis(8)))
                .thenReturn(Instant.EPOCH.plus(Duration.ofMillis(12)));
    }

    @Value.Immutable
    interface IntAndDuration {
        @Value.Parameter
        int number();
        @Value.Parameter
        Duration duration();
    }
}
