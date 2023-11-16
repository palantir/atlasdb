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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.AbstractObjectAssert;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
public class AsyncInitializerTest {
    private static final int ASYNC_INIT_DELAY = 10;

    private static class AlwaysFailingInitializer extends AsyncInitializer {
        final AtomicInteger initializationAttempts = new AtomicInteger(0);
        DeterministicSchedulerShutdownAware deterministicScheduler;

        @Override
        public void tryInitialize() {
            initializationAttempts.incrementAndGet();
            throw new RuntimeException("Failed initializing");
        }

        @Override
        protected Duration sleepInterval() {
            return Duration.ofMillis(10);
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

    private static final class AlwaysSucceedingInitializer extends AsyncInitializer {
        @Override
        protected void tryInitialize() {
            // do nothing
        }

        @Override
        protected String getInitializingClassName() {
            return AlwaysSucceedingInitializer.class.getName();
        }

        @Override
        public void cleanUpOnInitFailure() {
            throw new SafeIllegalStateException("Should never be called, since initialization does not fail.");
        }
    }

    private static final class DeterministicSchedulerShutdownAware extends DeterministicScheduler {
        private final AtomicInteger numberOfTimesShutdownCalled = new AtomicInteger(0);

        @Override
        public void shutdown() {
            numberOfTimesShutdownCalled.incrementAndGet();
        }
    }

    @Test
    public void synchronousInitializationDoesNotRunCleanupTasksOnSuccess() {
        // This is a bit crappy. However, the abstract class AsyncInitializer contains some internal state and it's
        // not easy to mock that internal state, hence we use a stub subclass that works.
        AsyncInitializer initializer = spy(new AlwaysSucceedingInitializer());
        assertThatCode(() -> initializer.initialize(false)).doesNotThrowAnyException();
        verify(initializer, never()).cleanUpOnInitFailure();
        verify(initializer, never()).scheduleInitialization(any());
    }

    @Test
    @MockitoSettings(strictness = Strictness.LENIENT)
    public void synchronousInitializationPropagatesExceptionsCleansUpAndDoesNotRetry() {
        AsyncInitializer initializer = getMockedInitializer();

        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed initializing");
        verify(initializer).tryInitialize();
        verify(initializer).cleanUpOnInitFailure();
        verify(initializer, never()).scheduleInitialization(any());
    }

    @Test
    @MockitoSettings(strictness = Strictness.LENIENT)
    public void synchronousInitializationPropagatesCleanupFailuresAsSuppressedExceptions() {
        AsyncInitializer initializer = getMockedInitializer();
        RuntimeException cleanupFailure = new RuntimeException("cleanup");
        doThrow(cleanupFailure).when(initializer).cleanUpOnInitFailure();

        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed initializing")
                .hasSuppressedException(cleanupFailure);
        verify(initializer).tryInitialize();
        verify(initializer).cleanUpOnInitFailure();
        verify(initializer, never()).scheduleInitialization(any());
    }

    @Test
    @MockitoSettings(strictness = Strictness.LENIENT)
    public void asyncInitializationCatchesExceptionAndRetries() {
        AsyncInitializer initializer = getMockedInitializer();

        initializer.initialize(true);
        verify(initializer, never()).tryInitialize();
        verify(initializer).scheduleInitialization(any());
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
        assertInitializer(initializer).isNotInitialized().hasAttempts(1);
    }

    @Test
    public void initializationAlwaysFailsAfterTheFirstAsynchronousTry() {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();

        initializer.initialize(true);
        assertThatThrownBy(() -> initializer.initialize(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Multiple calls tried to initialize the same instance.");
        assertInitializer(initializer).isNotInitialized().hasAttempts(0);

        tickScheduler(initializer, 0);
        assertInitializer(initializer).isNotInitialized().hasAttempts(1);

        tickScheduler(initializer, 5);
        assertInitializer(initializer).isNotInitialized().hasAttempts(6);
    }

    @Test
    public void asyncInitializationKeepsRetryingAndEventuallySucceeds() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                if (initializationAttempts.incrementAndGet() < 5) {
                    throw new RuntimeException("Failed initializing");
                }
            }
        };

        initializer.initialize(true);
        tickScheduler(initializer, 3);
        assertInitializer(initializer).isNotInitialized().hasAttempts(4);

        tickScheduler(initializer, 1);
        assertInitializer(initializer).isInitialized().hasAttempts(5);
    }

    @Test
    public void canCancelInitializationAndNoCleanupIfNotInitializedBetweenIterations() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer();
        Runnable cleanupTask = mock(Runnable.class);

        initializer.initialize(true);
        tickScheduler(initializer, 5);
        assertInitializer(initializer).isNotInitialized().hasAttempts(6).hasShutdowns(0);

        initializer.cancelInitialization(cleanupTask);
        tickScheduler(initializer, 5);
        assertInitializer(initializer).isNotInitialized().hasAttempts(6).hasShutdowns(1);

        verify(cleanupTask, never()).run();
    }

    @Test
    public void canCancelInitializationAndCleanupIfInitializedAfterCancel() throws InterruptedException {
        Runnable cleanupTask = mock(Runnable.class);
        doNothing().when(cleanupTask).run();
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                if (initializationAttempts.incrementAndGet() < 5) {
                    throw new RuntimeException("Failed initializing");
                }
                cancelInitialization(cleanupTask);
            }
        };

        initializer.initialize(true);
        tickScheduler(initializer, 3);
        assertInitializer(initializer).isNotInitialized().hasAttempts(4).hasShutdowns(0);

        // cancellation is called during the next run of tryInitialize
        tickScheduler(initializer, 1);
        assertInitializer(initializer).isNotInitialized().hasAttempts(5).hasShutdowns(1);

        verify(cleanupTask, times(1)).run();
    }

    @Test
    public void canCancelInitializationAndCleanupIfAlreadyInitialized() throws InterruptedException {
        AlwaysFailingInitializer initializer = new AlwaysFailingInitializer() {
            @Override
            public void tryInitialize() {
                initializationAttempts.incrementAndGet();
            }
        };
        Runnable cleanupTask = mock(Runnable.class);
        doNothing().when(cleanupTask).run();

        initializer.initialize(true);
        tickScheduler(initializer, 5);
        assertInitializer(initializer).isInitialized().hasAttempts(1).hasShutdowns(1);

        initializer.cancelInitialization(cleanupTask);
        tickScheduler(initializer, 1);
        assertInitializer(initializer).isInitialized().hasAttempts(1).hasShutdowns(1);

        verify(cleanupTask, times(1)).run();
    }

    private static AlwaysFailingInitializerAssert assertInitializer(AlwaysFailingInitializer initializer) {
        return new AlwaysFailingInitializerAssert(initializer);
    }

    private static final class AlwaysFailingInitializerAssert
            extends AbstractObjectAssert<AlwaysFailingInitializerAssert, AlwaysFailingInitializer> {

        private AlwaysFailingInitializerAssert(AlwaysFailingInitializer actual) {
            super(actual, AlwaysFailingInitializerAssert.class);
        }

        private AlwaysFailingInitializerAssert isInitialized() {
            assertThat(actual.isInitialized()).isTrue();
            return this;
        }

        private AlwaysFailingInitializerAssert isNotInitialized() {
            assertThat(actual.isInitialized()).isFalse();
            return this;
        }

        private AlwaysFailingInitializerAssert hasAttempts(int attempts) {
            assertThat(actual.initializationAttempts.get()).isEqualTo(attempts);
            return this;
        }

        private AlwaysFailingInitializerAssert hasShutdowns(int shutdowns) {
            assertThat(actual.deterministicScheduler.numberOfTimesShutdownCalled.get())
                    .isEqualTo(shutdowns);
            return this;
        }
    }

    private static AsyncInitializer getMockedInitializer() {
        AsyncInitializer initializer = mock(AsyncInitializer.class, Mockito.CALLS_REAL_METHODS);
        doNothing().when(initializer).assertNeverCalledInitialize();
        doThrow(new RuntimeException("Failed initializing")).when(initializer).tryInitialize();
        doNothing().when(initializer).scheduleInitialization(any());
        return initializer;
    }

    private static void tickScheduler(AlwaysFailingInitializer initializer, long times) {
        initializer.deterministicScheduler.tick(ASYNC_INIT_DELAY * times, TimeUnit.MILLISECONDS);
    }
}
