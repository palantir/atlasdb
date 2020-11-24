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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;
import org.mockito.Mockito;

public class AsyncInitializerTest {
    private static final int ASYNC_INIT_DELAY = 10;
    private static final int FIVE = 5;

    private static class AlwaysFailingInitializer extends AsyncInitializer {
        volatile int initializationAttempts = 0;
        DeterministicSchedulerShutdownAware deterministicScheduler;

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

        @Override
        ScheduledExecutorService createExecutorService() {
            deterministicScheduler = new DeterministicSchedulerShutdownAware();
            return deterministicScheduler;
        }
    }

    private static final class DeterministicSchedulerShutdownAware extends DeterministicScheduler {
        volatile int numberOfTimesShutdownCalled = 0;

        @Override
        public void shutdown() {
            numberOfTimesShutdownCalled++;
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
    public void initializationAlwaysFailsAfterTheFirstSynchronousTry() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();

        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed initializing");
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple calls tried to initialize the same instance.");
        tickSchedulerFiveTimes(initializer);
        assertThat(initializer.initializationAttempts).isEqualTo(1);
    }

    @Test
    public void initializationAlwaysFailsAfterTheFirstAsynchronousTry() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();

        initializer.initialize(true);
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple calls tried to initialize the same instance.");
        tickSchedulerFiveTimes(initializer);
        assertThat(initializer.initializationAttempts).isEqualTo(1 + FIVE);
    }

    @Test
    public void asyncInitializationKeepsRetryingAndEventuallySucceeds() throws InterruptedException {
        AlwaysFailingInitializer eventuallySuccessfulInitializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                if (initializationAttempts < FIVE) {
                    super.tryInitialize();
                }
            }
        };

        eventuallySuccessfulInitializer.initialize(true);
        assertThat(eventuallySuccessfulInitializer.isInitialized()).isFalse();
        tickSchedulerFiveTimes(eventuallySuccessfulInitializer);
        assertThat(eventuallySuccessfulInitializer.isInitialized()).isTrue();
    }

    @Test
    public void canCancelInitializationAndNoCleanupIfNotInitializedBetweenIterations() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();
        Runnable cleanupTask = mock(Runnable.class);

        initializer.initialize(true);
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(initializer, 0, 6);

        initializer.cancelInitialization(cleanupTask);
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(initializer, 1, 6);

        verify(cleanupTask, never()).run();
    }

    @Test
    public void canCancelInitializationAndCleanupIfInitializedAfterCancel() throws InterruptedException {
        Runnable cleanupTask = mock(Runnable.class);
        doNothing().when(cleanupTask).run();
        AlwaysFailingInitializer selfCancellingInitializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                if (++initializationAttempts <= 6) {
                    throw new RuntimeException("Fail 6 times");
                }
                cancelInitialization(cleanupTask);
            }
        };

        selfCancellingInitializer.initialize(true);
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(selfCancellingInitializer, 0, 6);

        // cancellation is called during the next run of tryInitialize
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(selfCancellingInitializer, 1, 7);

        verify(cleanupTask, times(1)).run();
    }

    @Test
    public void canCancelInitializationAndCleanupIfAlreadyInitialized() throws InterruptedException {
        AlwaysFailingInitializer successfulInitializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                ++initializationAttempts;
            }
        };
        Runnable cleanupTask = mock(Runnable.class);
        doNothing().when(cleanupTask).run();

        successfulInitializer.initialize(true);
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(successfulInitializer, 1, 1);

        successfulInitializer.cancelInitialization(cleanupTask);
        fiveTicksAndAssertNumberOfShutdownsAndAttempts(successfulInitializer, 1, 1);

        verify(cleanupTask, times(1)).run();
    }

    private void fiveTicksAndAssertNumberOfShutdownsAndAttempts(
            AlwaysFailingInitializer initializer, int shutdowns, int attempts) {
        tickSchedulerFiveTimes(initializer);
        assertThat(initializer.deterministicScheduler.numberOfTimesShutdownCalled)
                .isEqualTo(shutdowns);
        assertThat(initializer.initializationAttempts).isEqualTo(attempts);
    }

    private AsyncInitializer getMockedInitializer() {
        AsyncInitializer initializer = mock(AsyncInitializer.class, Mockito.CALLS_REAL_METHODS);
        doNothing().when(initializer).assertNeverCalledInitialize();
        doThrow(new RuntimeException("Failed initializing")).when(initializer).tryInitialize();
        doNothing().when(initializer).scheduleInitialization();
        return initializer;
    }

    private void tickSchedulerFiveTimes(AlwaysFailingInitializer initializer) {
        initializer.deterministicScheduler.tick(ASYNC_INIT_DELAY * FIVE + 1, TimeUnit.MILLISECONDS);
    }
}
