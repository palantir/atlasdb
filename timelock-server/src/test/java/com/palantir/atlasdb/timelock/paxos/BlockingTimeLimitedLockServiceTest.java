/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

import static org.mockito.Mockito.mock;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.util.concurrent.FakeTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.palantir.lock.LockService;

public class BlockingTimeLimitedLockServiceTest {
    private static final long BLOCKING_TIME_LIMIT_MILLIS = 10L;

    private final TimeLimiter acceptingLimiter = new FakeTimeLimiter();
    private final TimeLimiter timingOutLimiter = new ThrowingTimeLimiter(new UncheckedTimeoutException());
    private final TimeLimiter interruptingLimiter = new ThrowingTimeLimiter(new InterruptedException());
    private final TimeLimiter throwingLimiter = new ThrowingTimeLimiter(new IllegalStateException());

    private final LockService delegate = mock(LockService.class);

    private final LockService acceptingService = createService(acceptingLimiter);
    private final LockService timingOutService = createService(timingOutLimiter);
    private final LockService interruptingService = createService(interruptingLimiter);
    private final LockService throwingService = createService(throwingLimiter);

    @Test
    public void delegatesOperationsUnderTimeLimit() {

    }

    private LockService createService(TimeLimiter limiter) {
        return new BlockingTimeLimitedLockService(delegate, limiter, BLOCKING_TIME_LIMIT_MILLIS);
    }

    private static class ThrowingTimeLimiter implements TimeLimiter {
        private final Exception primedException;

        private ThrowingTimeLimiter(Exception primedException) {
            this.primedException = primedException;
        }

        @Override
        public <T> T newProxy(T target, Class<T> interfaceType, long timeoutDuration, TimeUnit timeoutUnit) {
            throw new UnsupportedOperationException("Not expecting to create a new proxy");
        }

        @Override
        public <T> T callWithTimeout(Callable<T> callable, long timeoutDuration, TimeUnit timeoutUnit,
                boolean interruptible) throws Exception {
            throw primedException;
        }
    }
}
