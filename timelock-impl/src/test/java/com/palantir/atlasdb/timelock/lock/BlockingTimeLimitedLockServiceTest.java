/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.org.lidalia.slf4jtest.LoggingEvent.info;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.FakeTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockClientAndThread;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockThreadInfoSnapshotManager;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.logsafe.UnsafeArg;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

@SuppressWarnings("DoNotMock") // we are just using mocks as proxies that always throw
public class BlockingTimeLimitedLockServiceTest {
    private static final long BLOCKING_TIME_LIMIT_MILLIS = 10L;
    private static final long TEST_CURRENT_TIME_MILLIS = 42L;
    private static final String CLIENT = "client";
    private static final String TEST_THREAD = "test-thread";

    private static final LockDescriptor TEST_LOCK = StringLockDescriptor.of("lockId");
    private static final LockRequest LOCK_REQUEST = LockRequest.builder(
                    ImmutableSortedMap.of(TEST_LOCK, LockMode.WRITE))
            .withCreatingThreadName(TEST_THREAD)
            .build();

    private final TimeLimiter acceptingLimiter = new FakeTimeLimiter();
    private final TimeLimiter timingOutLimiter = mock(TimeLimiter.class);
    private final TimeLimiter interruptingLimiter = mock(TimeLimiter.class);
    private final TimeLimiter uncheckedThrowingLimiter = mock(TimeLimiter.class);
    private final TimeLimiter checkedThrowingLimiter = mock(TimeLimiter.class);

    private final CloseableLockService delegate = mock(CloseableLockService.class);

    private final LockThreadInfoSnapshotManager recordingThreadInfoSnapshotManager =
            mock(LockThreadInfoSnapshotManager.class);
    private final LockThreadInfoSnapshotManager nonRecordingThreadInfoSnapshotManager =
            mock(LockThreadInfoSnapshotManager.class);

    private final BlockingTimeLimitedLockService acceptingService =
            createService(acceptingLimiter, nonRecordingThreadInfoSnapshotManager);
    private final BlockingTimeLimitedLockService timingOutAndNonRecordingService =
            createService(timingOutLimiter, nonRecordingThreadInfoSnapshotManager);
    private final BlockingTimeLimitedLockService interruptingService =
            createService(interruptingLimiter, nonRecordingThreadInfoSnapshotManager);
    private final BlockingTimeLimitedLockService uncheckedThrowingService =
            createService(uncheckedThrowingLimiter, nonRecordingThreadInfoSnapshotManager);
    private final BlockingTimeLimitedLockService checkedThrowingService =
            createService(checkedThrowingLimiter, nonRecordingThreadInfoSnapshotManager);
    private final BlockingTimeLimitedLockService timingOutAndRecordingService =
            createService(timingOutLimiter, recordingThreadInfoSnapshotManager);

    private static final String TIMEOUT_LOG_MESSAGE =
            "Lock service timed out after {} milliseconds when servicing {}" + " for client \"{}\"";
    private TestLogger testLogger;

    @Before
    public void setUp() throws Exception {
        when(timingOutLimiter.callWithTimeout(any(), anyLong(), any())).thenThrow(new TimeoutException());
        when(interruptingLimiter.callWithTimeout(any(), anyLong(), any())).thenThrow(new InterruptedException());
        when(uncheckedThrowingLimiter.callWithTimeout(any(), anyLong(), any()))
                .thenThrow(new UncheckedExecutionException(new IllegalStateException()));
        when(checkedThrowingLimiter.callWithTimeout(any(), anyLong(), any()))
                .thenThrow(new ExecutionException(new Exception()));
        when(recordingThreadInfoSnapshotManager.isRecordingThreadInfo()).thenReturn(true);
        when(nonRecordingThreadInfoSnapshotManager.isRecordingThreadInfo()).thenReturn(false);
        testLogger = TestLoggerFactory.getTestLogger(BlockingTimeLimitedLockService.class);
        testLogger.setEnabledLevelsForAllThreads(Level.INFO);
    }

    @Test
    public void delegatesInterruptibleOperations() throws InterruptedException {
        acceptingService.lock(CLIENT, LOCK_REQUEST);
        verify(delegate, times(1)).lock(CLIENT, LOCK_REQUEST);
    }

    @Test
    public void delegatesUninterruptibleOperations() {
        verifyCurrentTimeMillisDelegates(acceptingService);
    }

    @Test
    public void timeoutDoesNotApplyToUninterruptibleOperations() {
        verifyCurrentTimeMillisDelegates(timingOutAndNonRecordingService);
    }

    @Test
    public void interruptDoesNotApplyToUninterruptibleOperations() {
        verifyCurrentTimeMillisDelegates(interruptingService);
    }

    @Test
    public void throwsBlockingTimeoutExceptionIfInterruptibleOperationTimesOut() {
        assertThatThrownBy(() -> timingOutAndNonRecordingService.lock(CLIENT, LOCK_REQUEST))
                .isInstanceOf(BlockingTimeoutException.class);
    }

    @Test
    public void rethrowsInterruptedExceptionIfInterruptibleOperationIsInterrupted() {
        assertThatThrownBy(() -> interruptingService.lockAndGetHeldLocks(CLIENT, LOCK_REQUEST))
                .isInstanceOf(InterruptedException.class);
    }

    @Test
    public void rethrowsExceptionOccurringFromInterruptibleOperation() {
        assertThatThrownBy(() -> uncheckedThrowingService.lock(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> checkedThrowingService.lock(LockClient.ANONYMOUS.getClientId(), LOCK_REQUEST))
                .isInstanceOf(RuntimeException.class)
                .hasCauseExactlyInstanceOf(Exception.class);
    }

    @Test
    public void timeoutLogDoesNotIncludeThreadInfoIfNotRecording() {
        assertThatThrownBy(() -> timingOutAndNonRecordingService.lock(CLIENT, LOCK_REQUEST))
                .isInstanceOf(BlockingTimeoutException.class);
        assertContainsMatchingLoggingEvent(
                testLogger.getLoggingEvents(),
                info(TIMEOUT_LOG_MESSAGE, BLOCKING_TIME_LIMIT_MILLIS, "lock", CLIENT, LOCK_REQUEST));
    }

    @Test
    public void timeoutLogIncludesLastAcquiringThreadIfRecordingThreadInfo() {
        LockClientAndThread expectedClientThread = LockClientAndThread.of(LockClient.ANONYMOUS, TEST_THREAD);
        LockDescriptor testLock2 = StringLockDescriptor.of("test-lock-2");
        LockRequest lockRequest = LockRequest.builder(
                        ImmutableSortedMap.of(TEST_LOCK, LockMode.WRITE, testLock2, LockMode.WRITE))
                .withCreatingThreadName(TEST_THREAD)
                .build();

        Map<LockDescriptor, LockClientAndThread> expected =
                Map.of(TEST_LOCK, expectedClientThread, testLock2, expectedClientThread);
        when(recordingThreadInfoSnapshotManager.getRestrictedSnapshotAsLogArg(Set.of(TEST_LOCK, testLock2)))
                .thenReturn(UnsafeArg.of("presumedClientThreadHolders", expected));

        assertThatThrownBy(() -> timingOutAndRecordingService.lock(CLIENT, lockRequest))
                .isInstanceOf(BlockingTimeoutException.class);
        assertContainsMatchingLoggingEvent(
                testLogger.getLoggingEvents(),
                info(TIMEOUT_LOG_MESSAGE, BLOCKING_TIME_LIMIT_MILLIS, "lock", CLIENT, expected, lockRequest));
    }

    @Test
    public void timeoutLogIncludesAtMostNLocksIfRecordingThreadInfo() {
        SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();

        for (int i = 0; i < 2 * BlockingTimeLimitedLockService.MAX_THREADINFO_TO_LOG + 1; i++) {
            LockDescriptor lock = StringLockDescriptor.of("thread-lock-" + i);
            locks.put(lock, LockMode.WRITE);
        }

        LockRequest lockRequest = LockRequest.builder(locks).build();

        assertThatThrownBy(() -> timingOutAndRecordingService.lock(CLIENT, lockRequest))
                .isInstanceOf(BlockingTimeoutException.class);

        verify(recordingThreadInfoSnapshotManager).getRestrictedSnapshotAsLogArg(argThat(x -> {
            assertThat(x).hasSize(BlockingTimeLimitedLockService.MAX_THREADINFO_TO_LOG);
            return true;
        }));
    }

    @Test
    public void closesDelegate() throws IOException {
        acceptingService.close();

        verify(delegate).close();
    }

    private static void assertContainsMatchingLoggingEvent(List<LoggingEvent> actuals, LoggingEvent expected) {
        List<String> expectedParamStrings = extractArgumentsAsStringList(expected);
        assertThat(actuals.stream()
                        .filter(event -> event.getLevel() == expected.getLevel()
                                && event.getMessage().equals(expected.getMessage())
                                && expectedParamStrings.equals(extractArgumentsAsStringList(event)))
                        .collect(Collectors.toSet()))
                .hasSize(1);
    }

    private static List<String> extractArgumentsAsStringList(LoggingEvent event) {
        return event.getArguments().stream().map(Object::toString).collect(Collectors.toList());
    }

    private void verifyCurrentTimeMillisDelegates(BlockingTimeLimitedLockService service) {
        when(delegate.currentTimeMillis()).thenReturn(TEST_CURRENT_TIME_MILLIS);

        assertThat(service.currentTimeMillis()).isEqualTo(TEST_CURRENT_TIME_MILLIS);
        verify(delegate, times(1)).currentTimeMillis();
    }

    private BlockingTimeLimitedLockService createService(
            TimeLimiter limiter, LockThreadInfoSnapshotManager lockThreadInfoSnapshotManager) {
        return new BlockingTimeLimitedLockService(
                delegate, limiter, BLOCKING_TIME_LIMIT_MILLIS, lockThreadInfoSnapshotManager);
    }
}
