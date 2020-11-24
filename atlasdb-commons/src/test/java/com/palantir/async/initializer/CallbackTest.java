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
package com.palantir.async.initializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.palantir.common.concurrent.PTExecutors;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.awaitility.Awaitility;
import org.junit.Test;

public class CallbackTest {
    @Test
    public void runWithRetryStopsRetryingOnSuccess() {
        CountingCallback countingCallback = new CountingCallback(false);
        AtomicLong counter = new AtomicLong(0);

        countingCallback.runWithRetry(counter);

        assertThat(counter.get()).isEqualTo(10L);
    }

    @Test
    public void cleanupExceptionGetsPropagatedAndStopsRetrying() {
        CountingCallback countingCallback = new CountingCallback(true);
        AtomicLong counter = new AtomicLong(0);

        assertThatThrownBy(() -> countingCallback.runWithRetry(counter))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("LEGIT REASON");
        assertThat(counter.get()).isEqualTo(5L);
    }

    @Test
    public void shutdownDoesNotBlockWhenTaskIsNotRunning() {
        CountingCallback countingCallback = new CountingCallback(true);

        long start = System.currentTimeMillis();
        countingCallback.blockUntilSafeToShutdown();
        assertThat(System.currentTimeMillis()).isLessThanOrEqualTo(start + 100L);
    }

    @Test
    public void noOpCallbackDoesNotBlockOnRun() {
        Callback<Object> noOp = Callback.noOp();

        long start = System.currentTimeMillis();
        noOp.runWithRetry(new Object());
        assertThat(System.currentTimeMillis()).isLessThanOrEqualTo(start + 100L);
    }

    @Test
    public void noOpCallbackDoesNotBlockClosing() {
        Callback<Object> noOp = Callback.noOp();
        long start = System.currentTimeMillis();

        PTExecutors.newSingleThreadScheduledExecutor().execute(() -> noOp.runWithRetry(new Object()));

        noOp.blockUntilSafeToShutdown();
        assertThat(System.currentTimeMillis()).isLessThanOrEqualTo(start + 500L);
    }

    @Test
    public void shutdownBlocksWhenTaskIsRunningUntilCleanupIsDone() {
        SlowCallback slowCallback = new SlowCallback();
        long start = System.currentTimeMillis();
        AtomicBoolean started = new AtomicBoolean(false);

        PTExecutors.newSingleThreadScheduledExecutor().execute(() -> slowCallback.runWithRetry(started));
        Awaitility.waitAtMost(Duration.ofMillis(500L)).until(started::get);

        slowCallback.blockUntilSafeToShutdown();
        assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(start + 2000L);
    }

    private static class CountingCallback extends Callback<AtomicLong> {
        private final boolean throwOnLegitReason;

        CountingCallback(boolean throwOnLegitReason) {
            this.throwOnLegitReason = throwOnLegitReason;
        }

        @Override
        public void init(AtomicLong counter) {
            counter.incrementAndGet();
            if (counter.get() < 5L) {
                throw new Error("RANDOM REASON");
            }
            if (counter.get() < 10L) {
                throw new RuntimeException("LEGIT REASON");
            }
        }

        @Override
        public void cleanup(AtomicLong counter, Throwable initException) {
            if (throwOnLegitReason) {
                if (initException.getMessage().contains("LEGIT REASON")) {
                    throw (RuntimeException) initException;
                }
            }
        }
    }

    private static final class SlowCallback extends Callback<AtomicBoolean> {
        @Override
        public void init(AtomicBoolean started) {
            try {
                started.set(true);
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
            throw new RuntimeException();
        }

        @Override
        public void cleanup(AtomicBoolean started, Throwable initException) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
        }
    }
}
