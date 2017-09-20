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
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import com.jayway.awaitility.Awaitility;

public class AsyncInitializerTest {
    public static final int ASYNC_INIT_DELAY = 10;

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

        @Override
        protected String getInitializingClassName() {
            return "AlwaysFailingInitializer";
        }
    }

    @Test
    public void synchronousInitializationPropagatesExceptionsAndDoesNotRetry() throws InterruptedException {
        AsyncInitializer initializer = getMockedInitializer();

        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed initializing");
        verify(initializer).tryInitialize();
        verify(initializer, never()).scheduleInitialization();
    }

    @Test
    public void asyncInitializationCatchesExceptionAndRetries() {
        AsyncInitializer initializer = getMockedInitializer();

        initializer.initialize(true);
        verify(initializer).tryInitialize();
        verify(initializer).scheduleInitialization();
    }

    @Test
    public void initializationAlwaysFailsAfterTheFirstAsynchronousTry() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();

        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .withFailMessage("Failed initializing");
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple calls tried to initialize the same instance.");
        assertThat(initializer.initializationAttempts).isEqualTo(1);
    }

    @Test
    public void initializationAlwaysFailsAfterTheFirstSynchronousTry() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();

        initializer.initialize(true);
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple calls tried to initialize the same instance.");
        assertThat(initializer.initializationAttempts).isEqualTo(1);
    }

    @Test
    public void asyncInitializationKeepsRetryingAndEventuallySucceeds() throws InterruptedException {
        AsyncInitializer eventuallySuccessfulInitializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                if (initializationAttempts < 5) {
                    super.tryInitialize();
                }
            }
        };

        eventuallySuccessfulInitializer.initialize(true);
        assertFalse(eventuallySuccessfulInitializer.isInitialized());
        Awaitility.with()
                .pollInterval(ASYNC_INIT_DELAY, TimeUnit.MILLISECONDS)
                .atMost(ASYNC_INIT_DELAY * 20, TimeUnit.MILLISECONDS)
                .until(() -> eventuallySuccessfulInitializer.isInitialized());
    }

    @Test
    public void canCancelInitializationAndPerformCleanupIfInitialized() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();
        AlwaysFailingInitializer successfulInitializer = new AlwaysFailingInitializer() {
            @Override
            public boolean isInitialized() {
                return true;
            }
        };
        Runnable cleanupTask = mock(Runnable.class);
        doNothing().when(cleanupTask).run();

        initializeAsyncCancelAndVerifyCancelled(initializer, cleanupTask);
        verify(cleanupTask, never()).run();

        initializeAsyncCancelAndVerifyCancelled(successfulInitializer, cleanupTask);
        verify(cleanupTask).run();
    }

    private AsyncInitializer getMockedInitializer() {
        AsyncInitializer initializer = mock(AsyncInitializer.class, Mockito.CALLS_REAL_METHODS);
        doNothing().when(initializer).assertNeverCalledInitialize();
        doThrow(new RuntimeException("Failed initializing")).when(initializer).tryInitialize();
        doNothing().when(initializer).scheduleInitialization();
        return initializer;
    }

    private void initializeAsyncCancelAndVerifyCancelled(AlwaysFailingInitializer initializer,
            Runnable cleanupTask) throws InterruptedException {
        initializer.initialize(true);
        initializer.cancelInitialization(cleanupTask);
        int numberOfAttemptsWhenCancelled = initializer.initializationAttempts;
        Thread.sleep(ASYNC_INIT_DELAY * 5);
        assertThat(initializer.initializationAttempts).isEqualTo(numberOfAttemptsWhenCancelled);
    }
}
