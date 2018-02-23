/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.async.initializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;

import com.palantir.common.concurrent.PTExecutors;

public class CallbackTest {
    @Test
    public void runWithRetryStopsRetryingOnSuccess() {
        CountingCallback countingCallback = new CountingCallback(false);
        countingCallback.runWithRetry();

        assertThat(countingCallback.initCounter).isEqualTo(10L);
    }

    @Test
    public void cleanupExceptionGetsPropagatedAndStopsRetrying() {
        CountingCallback countingCallback = new CountingCallback(true);

        assertThatThrownBy(() -> countingCallback.runWithRetry())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("LEGIT REASON");
        assertThat(countingCallback.initCounter).isEqualTo(5L);
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
        Callback noOp = Callback.NO_OP;

        long start = System.currentTimeMillis();
        noOp.runWithRetry();
        assertThat(System.currentTimeMillis()).isLessThanOrEqualTo(start + 100L);
    }

    @Test
    public void noOpCallbackDoesNotBlockClosing() {
        Callback noOp = Callback.NO_OP;
        long start = System.currentTimeMillis();

        PTExecutors.newSingleThreadScheduledExecutor().submit(noOp::runWithRetry);

        noOp.blockUntilSafeToShutdown();
        assertThat(System.currentTimeMillis()).isLessThanOrEqualTo(start + 500L);
    }

    @Test
    public void shutdownDoesBlocksWhenTaskIsRunningUntilCleanupIsDone() {
        SlowCallback slowCallback = new SlowCallback();
        long start = System.currentTimeMillis();

        PTExecutors.newSingleThreadScheduledExecutor().submit(slowCallback::runWithRetry);
        Awaitility.waitAtMost(200L, TimeUnit.MILLISECONDS).until(() -> slowCallback.started);

        slowCallback.blockUntilSafeToShutdown();
        assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(start + 2000L);
    }


    private static class CountingCallback extends Callback {
        private final boolean throwOnLegitReason;
        public volatile long initCounter = 0L;

        CountingCallback(boolean throwOnLegitReason) {
            this.throwOnLegitReason = throwOnLegitReason;
        }

        @Override
        public void init() {
            initCounter++;
            if (initCounter < 5L) {
                throw new RuntimeException("RANDOM REASON");
            }
            if (initCounter < 10L) {
                throw new RuntimeException("LEGIT REASON");
            }
        }

        @Override
        public void cleanup(Exception initException) {
            if (throwOnLegitReason) {
                if (initException.getMessage().contains("LEGIT REASON")) {
                    throw (RuntimeException) initException;
                }
            }
        }
    }

    private static class SlowCallback extends Callback {
        volatile boolean started = false;

        @Override
        public void init() {
            try {
                started = true;
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
            throw new RuntimeException();
        }

        @Override
        public void cleanup(Exception initException) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
        }
    }
}
