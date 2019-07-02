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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.logsafe.Arg;

public class ProfilingTimelockServiceTest {
    private static final Duration SHORT_DURATION = ProfilingTimelockService.SLOW_THRESHOLD.dividedBy(5);
    private static final Duration LONG_DURATION = ProfilingTimelockService.SLOW_THRESHOLD.multipliedBy(5);
    private static final Duration TWO_CENTURIES = Duration.ofDays(365 * 100 * 2 + 50 - 2); // most of the time

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
    private final BooleanSupplier loggingPermissionSupplier = () -> allowLogging.getAndSet(false);

    private final ProfilingTimelockService profilingTimelockService = new ProfilingTimelockService(
            logger, delegate, () -> Stopwatch.createStarted(ticker), loggingPermissionSupplier);

    @Test
    public void doesNotLogIfOperationsAreFast() {
        makeOperationsTakeSpecifiedDuration(SHORT_DURATION);
        allowLogging();
        profilingTimelockService.getFreshTimestamp();
        profilingTimelockService.startIdentifiedAtlasDbTransaction();

        verify(delegate).getFreshTimestamp();
        verify(delegate).startIdentifiedAtlasDbTransaction();
        verifyLoggerNeverInvoked();
    }

    @Test
    public void logsIfOperationsAreSlowAndLoggingAllowed() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        allowLogging();
        profilingTimelockService.getFreshTimestamp();

        verify(delegate).getFreshTimestamp();
        verifyLoggerInvokedWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
    }

    @Test
    public void doesNotLogIfLoggingNotAllowedEvenIfOperationsAreSlow() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        profilingTimelockService.getFreshTimestamp();

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

        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        allowLogging();
        assertThatThrownBy(profilingTimelockService::getFreshTimestamp).isEqualTo(exception);

        verify(delegate).getFreshTimestamp();
        verifyLoggerInvokedWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
    }

    @Test
    public void doesNotLogIfLockIsSlow() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        allowLogging();
        profilingTimelockService.lock(LockRequest.of(ImmutableSet.of(StringLockDescriptor.of("exclusive")), 1234));

        verifyLoggerNeverInvoked();
    }

    @Test
    public void doesNotLogIfWaitForLocksIsSlow() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        allowLogging();
        profilingTimelockService.waitForLocks(WaitForLocksRequest.of(
                ImmutableSet.of(StringLockDescriptor.of("combination")), 123));

        verifyLoggerNeverInvoked();
    }

    @Test
    public void logsSlowestOperationIfMultipleOperationsExceedTheSlowThresholdIfFirst() {
        makeOperationsTakeSpecifiedDuration(TWO_CENTURIES);
        profilingTimelockService.getFreshTimestamp();

        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        allowLogging();
        profilingTimelockService.startIdentifiedAtlasDbTransaction();

        verifyLoggerInvokedWithSpecificProfile("getFreshTimestamp", TWO_CENTURIES);
    }

    @Test
    public void logsSlowestOperationIfMultipleOperationsExceedTheSlowThresholdIfNotFirst() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        profilingTimelockService.getFreshTimestamp();

        makeOperationsTakeSpecifiedDuration(TWO_CENTURIES);
        allowLogging();
        profilingTimelockService.startIdentifiedAtlasDbTransaction();

        verifyLoggerInvokedWithSpecificProfile("startIdentifiedAtlasDbTransaction", TWO_CENTURIES);
    }

    @Test
    public void logsTheSlowestNonBannedOperationAboveThreshold() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        profilingTimelockService.getFreshTimestamp();

        makeOperationsTakeSpecifiedDuration(TWO_CENTURIES);
        allowLogging();
        profilingTimelockService.lock(LockRequest.of(ImmutableSet.of(StringLockDescriptor.of("mortice")), 2366));

        verifyLoggerInvokedWithSpecificProfile("getFreshTimestamp", LONG_DURATION);
    }

    @Test
    public void logsOnlyOnceEvenIfManyRequestsAreSlow() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        for (int i = 0; i < 100; i++) {
            profilingTimelockService.lockImmutableTimestamp();
        }

        makeOperationsTakeSpecifiedDuration(SHORT_DURATION);
        allowLogging();
        profilingTimelockService.startIdentifiedAtlasDbTransaction();

        verifyLoggerInvokedWithSpecificProfile("lockImmutableTimestamp", LONG_DURATION);
        verifyNoMoreInteractions(logger);
    }

    @Test
    public void logsMultipleTimesIfAllowedToMultipleTimes() {
        makeOperationsTakeSpecifiedDuration(LONG_DURATION);
        for (int i = 0; i < 100; i++) {
            allowLogging();
            profilingTimelockService.lockImmutableTimestamp();
            profilingTimelockService.lockImmutableTimestamp();
            profilingTimelockService.lockImmutableTimestamp();
        }
        verifyLoggerInvokedSpecificNumberOfTimes(100);
    }

    private void verifyLoggerNeverInvoked() {
        verifyNoMoreInteractions(logger);
    }

    @SuppressWarnings("unchecked") // Captors of generics
    @SuppressWarnings("Slf4jConstantLogMessage") // only for tests
    private void verifyLoggerInvokedWithSpecificProfile(String operation, Duration duration) {
        // XXX Maybe there's a better way? The approaches I tried didn't work because of conflict with other methods
        ArgumentCaptor<Arg<String>> messageCaptor = ArgumentCaptor.forClass(Arg.class);
        ArgumentCaptor<Arg<Duration>> durationCaptor = ArgumentCaptor.forClass(Arg.class);
        verify(logger).info(
                any(String.class), messageCaptor.capture(), durationCaptor.capture(), any(), any(), any(), any());

        assertThat(messageCaptor.getValue().getValue()).isEqualTo(operation);
        assertThat(durationCaptor.getValue().getValue()).isEqualTo(duration);
    }

    @SuppressWarnings("Slf4jConstantLogMessage") // only for tests
    private void verifyLoggerInvokedSpecificNumberOfTimes(int times) {
        verify(logger, times(times)).info(any(String.class), any(), any(), any(), any(), any(), any());
    }

    private void makeOperationsTakeSpecifiedDuration(Duration duration) {
        operationTiming = duration.toNanos();
    }

    private void allowLogging() {
        allowLogging.set(true);
    }
}
