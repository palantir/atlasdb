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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.Arg;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

public class ProfilingTimelockServiceTest {
    private static final Duration SHORT_DURATION = ProfilingTimelockService.SLOW_THRESHOLD.dividedBy(5);
    private static final Duration LONG_DURATION = ProfilingTimelockService.SLOW_THRESHOLD.multipliedBy(5);
    private static final Duration TWO_CENTURIES =
            ChronoUnit.CENTURIES.getDuration().multipliedBy(2);

    @SuppressWarnings("PreferStaticLoggers")
    private final Logger logger = mock(Logger.class);

    private final TimelockService delegate = mock(TimelockService.class);

    private long clock = 0;
    private long operationTiming = 0;
    private final Ticker ticker = new Ticker() {
        @Override
        public long read() {
            clock += operationTiming;
            return clock;
        }
    };

    private final AtomicBoolean allowLogging = new AtomicBoolean();
    private final BooleanSupplier loggingPermissionSupplier = allowLogging::get;

    private final ProfilingTimelockService profilingTimelockService = new ProfilingTimelockService(
            logger, delegate, () -> Stopwatch.createStarted(ticker), loggingPermissionSupplier);

    @Test
    public void delegatesInitializationCheck() {
        when(delegate.isInitialized()).thenReturn(false).thenReturn(true);

        assertThat(profilingTimelockService.isInitialized()).isFalse();
        assertThat(profilingTimelockService.isInitialized()).isTrue();
    }

    @Test
    public void doesNotLogIfOperationsAreFast() {
        flushLogsWithCall(SHORT_DURATION, profilingTimelockService::getFreshTimestamp);
        flushLogsWithCall(SHORT_DURATION, () -> profilingTimelockService.startIdentifiedAtlasDbTransactionBatch(1));
        flushLogsWithCall(SHORT_DURATION, () -> profilingTimelockService.startIdentifiedAtlasDbTransactionBatch(999));

        verify(delegate).getFreshTimestamp();
        verify(delegate).startIdentifiedAtlasDbTransactionBatch(1);
        verifyLoggerNeverInvoked();
    }

    @Test
    public void logsIfOperationsAreSlowAndLoggingAllowed() {
        flushLogsWithCall(LONG_DURATION, profilingTimelockService::getFreshTimestamp);

        verify(delegate).getFreshTimestamp();
        verifyLoggerInvokedOnceWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
    }

    @Test
    public void doesNotLogIfLoggingNotAllowedEvenIfOperationsAreSlow() {
        accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::getFreshTimestamp);

        verify(delegate).getFreshTimestamp();
        verifyLoggerNeverInvoked();
    }

    @Test
    public void returnsResultReturnedByDelegate() {
        when(profilingTimelockService.getFreshTimestamp()).thenReturn(42L);
        assertThat(profilingTimelockService.getFreshTimestamp()).isEqualTo(42L);
    }

    @Test
    public void logsIfFailedOperationsAreSlow() {
        RuntimeException exception = new RuntimeException("kaboom");
        when(profilingTimelockService.getFreshTimestamp()).thenThrow(exception);

        assertThatThrownBy(() -> flushLogsWithCall(LONG_DURATION, profilingTimelockService::getFreshTimestamp))
                .isEqualTo(exception);

        verify(delegate).getFreshTimestamp();
        verifyLoggerInvokedOnceWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
        verifyLoggerInvokedOnceWithException(exception);
    }

    @Test
    public void doesNotLogIfLockIsSlow() {
        flushLogsWithCall(
                LONG_DURATION,
                () -> profilingTimelockService.lock(
                        LockRequest.of(ImmutableSet.of(StringLockDescriptor.of("exclusive")), 1234)));

        verifyLoggerNeverInvoked();
    }

    @Test
    public void doesNotLogIfWaitForLocksIsSlow() {
        flushLogsWithCall(
                LONG_DURATION,
                () -> profilingTimelockService.waitForLocks(
                        WaitForLocksRequest.of(ImmutableSet.of(StringLockDescriptor.of("combination")), 123)));

        verifyLoggerNeverInvoked();
    }

    @Test
    public void logsSlowestOperationIfMultipleOperationsExceedTheSlowThresholdIfFirst() {
        accumulateLogsWithCall(TWO_CENTURIES, profilingTimelockService::getFreshTimestamp);
        flushLogsWithCall(LONG_DURATION, () -> profilingTimelockService.startIdentifiedAtlasDbTransactionBatch(1));

        verifyLoggerInvokedOnceWithSpecificProfile("getFreshTimestamp", TWO_CENTURIES);
    }

    @Test
    public void logsSlowestOperationIfMultipleOperationsExceedTheSlowThresholdIfNotFirst() {
        accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::getFreshTimestamp);
        flushLogsWithCall(TWO_CENTURIES, () -> profilingTimelockService.startIdentifiedAtlasDbTransactionBatch(1));

        verifyLoggerInvokedOnceWithSpecificProfile("startIdentifiedAtlasDbTransactionBatch", TWO_CENTURIES);
    }

    @Test
    public void logsTheSlowestNonBannedOperationAboveThreshold() {
        accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::getFreshTimestamp);

        flushLogsWithCall(
                TWO_CENTURIES,
                () -> profilingTimelockService.lock(
                        LockRequest.of(ImmutableSet.of(StringLockDescriptor.of("mortice")), 2366)));

        verifyLoggerInvokedOnceWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
    }

    @Test
    public void logsOnlyOnceEvenIfManyRequestsAreSlow() {
        for (int i = 0; i < 100; i++) {
            accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::lockImmutableTimestamp);
        }

        flushLogsWithCall(SHORT_DURATION, () -> profilingTimelockService.startIdentifiedAtlasDbTransactionBatch(1));

        verifyLoggerInvokedOnceWithSpecificProfile("lockImmutableTimestamp", LONG_DURATION);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logsMultipleTimesIfAllowedToMultipleTimes() {
        for (int i = 0; i < 100; i++) {
            accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::lockImmutableTimestamp);
            accumulateLogsWithCall(LONG_DURATION, profilingTimelockService::lockImmutableTimestamp);
            flushLogsWithCall(LONG_DURATION, profilingTimelockService::lockImmutableTimestamp);
        }
        verifyLoggerInvokedSpecificNumberOfTimes(100);
    }

    private void verifyLoggerNeverInvoked() {
        verifyNoMoreInteractions(logger);
    }

    @SuppressWarnings({
        "unchecked", // Captors of generics
        "Slf4jConstantLogMessage"
    }) // Logger verify
    private void verifyLoggerInvokedOnceWithSpecificProfile(String operation, Duration duration) {
        // XXX Maybe there's a better way? The approaches I tried didn't work because of conflict with other methods
        ArgumentCaptor<Arg<String>> messageCaptor = ArgumentCaptor.forClass(Arg.class);
        ArgumentCaptor<Arg<Duration>> durationCaptor = ArgumentCaptor.forClass(Arg.class);
        verify(logger).info(any(String.class), messageCaptor.capture(), durationCaptor.capture(), any(), any(), any());

        assertThat(messageCaptor.getValue().getValue()).isEqualTo(operation);
        assertThat(durationCaptor.getValue().getValue()).isEqualTo(duration);
    }

    @SuppressWarnings({
        "unchecked", // Captors of generics
        "Slf4jConstantLogMessage"
    }) // Logger verify
    private void verifyLoggerInvokedOnceWithException(Exception exception) {
        ArgumentCaptor<Arg<Optional<Exception>>> exceptionCaptor = ArgumentCaptor.forClass(Arg.class);
        verify(logger).info(any(String.class), any(), any(), any(), any(), exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue().getValue()).contains(exception);
    }

    @SuppressWarnings("Slf4jConstantLogMessage") // only for tests
    private void verifyLoggerInvokedSpecificNumberOfTimes(int times) {
        verify(logger, times(times)).info(any(String.class), any(), any(), any(), any(), any());
    }

    private void makeOperationsTakeSpecifiedDuration(Duration duration) {
        operationTiming = duration.toNanos();
    }

    private void accumulateLogsWithCall(Duration duration, Runnable operation) {
        allowLogging.set(false);
        runOperationUnderDuration(duration, operation);
    }

    private void flushLogsWithCall(Duration duration, Runnable operation) {
        allowLogging.set(true);
        runOperationUnderDuration(duration, operation);
    }

    private void runOperationUnderDuration(Duration duration, Runnable operation) {
        makeOperationsTakeSpecifiedDuration(duration);
        operation.run();
    }
}
