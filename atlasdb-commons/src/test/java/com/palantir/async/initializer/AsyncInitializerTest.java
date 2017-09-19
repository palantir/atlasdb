/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.jayway.awaitility.Awaitility;

public class AsyncInitializerTest {

    public static final int WAIT_TIME_MILLIS = 500;
    public static final int ASYNC_INIT_DELAY = WAIT_TIME_MILLIS / 10;

    private class AlwaysFailingInitializer extends AsyncInitializer {
        volatile int initializationAttempts = 0;

        @Override
        public void tryInitialize() {
            ++initializationAttempts;
            throw new RuntimeException("Failed initializing");
        }

        @Override
        protected int sleepIntervalInMillis() {
            return ASYNC_INIT_DELAY;
        }
    }

    @Test
    public void synchronousInitializationPropagatesExceptionsAndDoesNotRetry() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed initializing");
        Thread.sleep(WAIT_TIME_MILLIS);
        assertThat(initializer.initializationAttempts).isEqualTo(1);
    }

    @Test
    public void asyncInitializationCatchesExceptionAndRetries() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();
        initializer.initialize(true);
        Awaitility.await().pollInterval(ASYNC_INIT_DELAY, TimeUnit.MILLISECONDS)
                .atMost(WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS)
                .until(() -> initializer.initializationAttempts == 5);
    }

    @Test
    public void initializationIsNoopIfAlreadyInitialized() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer() {
            @Override
            public boolean isInitialized() {
                return true;
            }
        };
        initializer.initialize(true);
        assertThat(initializer.initializationAttempts).isEqualTo(0);
        initializer.initialize(false);
        assertThat(initializer.initializationAttempts).isEqualTo(0);
    }
}
