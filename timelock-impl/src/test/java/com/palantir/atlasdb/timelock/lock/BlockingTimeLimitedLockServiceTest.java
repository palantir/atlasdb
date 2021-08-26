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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.FakeTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.remoting.BlockingTimeoutException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("DoNotMock") // we are just using mocks as proxies that always throw
public class BlockingTimeLimitedLockServiceTest {
    private static final long BLOCKING_TIME_LIMIT_MILLIS = 10L;
    private static final long TEST_CURRENT_TIME_MILLIS = 42L;
    private static final String CLIENT = "client";
    private static final LockRequest LOCK_REQUEST = LockRequest.builder(
                    ImmutableSortedMap.of(StringLockDescriptor.of("lockId"), LockMode.WRITE))
            .build();

    private final TimeLimiter acceptingLimiter = new FakeTimeLimiter();
    private final TimeLimiter timingOutLimiter = mock(TimeLimiter.class);
    private final TimeLimiter interruptingLimiter = mock(TimeLimiter.class);
    private final TimeLimiter uncheckedThrowingLimiter = mock(TimeLimiter.class);
    private final TimeLimiter checkedThrowingLimiter = mock(TimeLimiter.class);

    private final CloseableLockService delegate = mock(CloseableLockService.class);

    private final BlockingTimeLimitedLockService acceptingService = createService(acceptingLimiter);
    private final BlockingTimeLimitedLockService timingOutService = createService(timingOutLimiter);
    private final BlockingTimeLimitedLockService interruptingService = createService(interruptingLimiter);
    private final BlockingTimeLimitedLockService uncheckedThrowingService = createService(uncheckedThrowingLimiter);
    private final BlockingTimeLimitedLockService checkedThrowingService = createService(checkedThrowingLimiter);

    @Before
    public void setUp() throws Exception {
        when(timingOutLimiter.callWithTimeout(any(), anyLong(), any())).thenThrow(new TimeoutException());
        when(interruptingLimiter.callWithTimeout(any(), anyLong(), any())).thenThrow(new InterruptedException());
        when(uncheckedThrowingLimiter.callWithTimeout(any(), anyLong(), any()))
                .thenThrow(new UncheckedExecutionException(new IllegalStateException()));
        when(checkedThrowingLimiter.callWithTimeout(any(), anyLong(), any()))
                .thenThrow(new ExecutionException(new Exception()));
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
        verifyCurrentTimeMillisDelegates(timingOutService);
    }

    @Test
    public void interruptDoesNotApplyToUninterruptibleOperations() {
        verifyCurrentTimeMillisDelegates(interruptingService);
    }

    @Test
    public void throwsBlockingTimeoutExceptionIfInterruptibleOperationTimesOut() {
        assertThatThrownBy(() -> timingOutService.lock(CLIENT, LOCK_REQUEST))
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
    public void closesDelegate() throws IOException {
        acceptingService.close();

        verify(delegate).close();
    }

    private void verifyCurrentTimeMillisDelegates(BlockingTimeLimitedLockService service) {
        when(delegate.currentTimeMillis()).thenReturn(TEST_CURRENT_TIME_MILLIS);

        assertThat(service.currentTimeMillis()).isEqualTo(TEST_CURRENT_TIME_MILLIS);
        verify(delegate, times(1)).currentTimeMillis();
    }

    private BlockingTimeLimitedLockService createService(TimeLimiter limiter) {
        return new BlockingTimeLimitedLockService(delegate, limiter, BLOCKING_TIME_LIMIT_MILLIS);
    }
}
