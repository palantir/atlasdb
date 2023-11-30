/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.base.Ticker;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.DetachedSpan;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class TimedDetachedSpanTest {
    @Mock
    private DetachedSpan delegate;

    @Test
    public void completeCompletesDelegateSpan() {
        TimedDetachedSpan span = TimedDetachedSpan.from(Ticker.systemTicker(), delegate);
        span.complete();
        verify(delegate).complete();
    }

    @Test
    public void queryingDurationDoesNotCompleteDelegateAndThrowsWhileSpanIsNotCompleted() {
        TimedDetachedSpan span = TimedDetachedSpan.from(Ticker.systemTicker(), delegate);
        assertThatLoggableExceptionThrownBy(span::getDurationOrThrowIfStillRunning)
                .isInstanceOf(SafeRuntimeException.class)
                .hasLogMessage("Fetching duration was attempted while the span task is still running.")
                .hasNoArgs();

        // checking twice to ensure that this call does not accidentally mark the span as completed before throwing
        assertThatLoggableExceptionThrownBy(span::getDurationOrThrowIfStillRunning)
                .isInstanceOf(SafeRuntimeException.class)
                .hasLogMessage("Fetching duration was attempted while the span task is still running.")
                .hasNoArgs();

        verify(delegate, never()).complete();
    }

    @Test
    public void spanDurationIsMeasuredFromCreationToCompletion() {
        FakeTicker fakeTicker = new FakeTicker();
        TimedDetachedSpan span = TimedDetachedSpan.from(fakeTicker, delegate);
        fakeTicker.advance(10, TimeUnit.MINUTES);
        span.complete();
        assertThat(span.getDurationOrThrowIfStillRunning()).isCloseTo(Duration.ofMinutes(10), Duration.ZERO);
    }

    @Test
    public void returnedSpanDurationIsNotAffectedByAdditionalCallsToComplete() {
        FakeTicker fakeTicker = new FakeTicker();
        TimedDetachedSpan span = TimedDetachedSpan.from(fakeTicker, delegate);
        fakeTicker.advance(10, TimeUnit.MINUTES);
        span.complete();
        assertThat(span.getDurationOrThrowIfStillRunning()).isCloseTo(Duration.ofMinutes(10), Duration.ZERO);
        span.complete();
        assertThat(span.getDurationOrThrowIfStillRunning()).isCloseTo(Duration.ofMinutes(10), Duration.ZERO);
    }
}
