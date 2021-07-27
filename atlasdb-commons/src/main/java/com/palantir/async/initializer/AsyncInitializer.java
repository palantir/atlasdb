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

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Implements basic infrastructure to allow an object to be asynchronously initialized.
 * In order to be ThreadSafe, the abstract methods of the inheriting class need to be synchronized.
 */
@ThreadSafe
public abstract class AsyncInitializer {
    private static final SafeLogger log = SafeLoggerFactory.get(AsyncInitializer.class);

    private final ScheduledExecutorService singleThreadedExecutor = createExecutorService();
    private final AtomicBoolean isInitializing = new AtomicBoolean(false);
    private AsyncInitializationState state = new AsyncInitializationState();
    private int numberOfInitializationAttempts = 1;
    private Long initializationStartTime;

    /**
     * Initialization method that must be called to initialize the object before it is used.
     * @param initializeAsync If true, the object will be initialized asynchronously when synchronous initialization
     * fails.
     */
    public final void initialize(boolean initializeAsync) {
        assertNeverCalledInitialize();

        initializationStartTime = System.currentTimeMillis();

        if (!initializeAsync) {
            tryInitializeInternal();
            return;
        }

        tryInitializationLoop();
    }

    private void tryInitializationLoop() {
        try {
            tryInitializeInternal();
            log.info(
                    "Initialized {} on the attempt {} in {} milliseconds",
                    SafeArg.of("className", getInitializingClassName()),
                    SafeArg.of("numberOfAttempts", numberOfInitializationAttempts),
                    SafeArg.of("initializationDuration", System.currentTimeMillis() - initializationStartTime));
        } catch (Throwable throwable) {
            log.info(
                    "Failed to initialize {} on the attempt {}",
                    SafeArg.of("className", getInitializingClassName()),
                    SafeArg.of("numberOfAttempts", numberOfInitializationAttempts++),
                    throwable);
            try {
                cleanUpOnInitFailure();
            } catch (Throwable cleanupThrowable) {
                log.error(
                        "Failed to cleanup when initialization of {} failed on attempt {} with {} milliseconds",
                        SafeArg.of("className", getInitializingClassName()),
                        SafeArg.of("numberOfAttempts", numberOfInitializationAttempts),
                        SafeArg.of("initializationDuration", System.currentTimeMillis() - initializationStartTime),
                        cleanupThrowable);
            }
            scheduleInitialization();
        }
    }

    // Not final for tests.
    void scheduleInitialization() {
        singleThreadedExecutor.schedule(
                () -> {
                    if (state.isCancelled()) {
                        singleThreadedExecutor.shutdown();
                        return;
                    }

                    tryInitializationLoop();
                },
                sleepIntervalInMillis(),
                TimeUnit.MILLISECONDS);
    }

    // Not final for tests
    ScheduledExecutorService createExecutorService() {
        return PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("AsyncInitializer-" + getInitializingClassName(), true));
    }

    // Not final for tests.
    void assertNeverCalledInitialize() {
        if (!isInitializing.compareAndSet(false, true)) {
            throw new IllegalStateException("Multiple calls tried to initialize the same instance.\n"
                    + "Each instance should have a single thread trying to initialize it.\n"
                    + "Object being initialized multiple times: " + getInitializingClassName());
        }
    }

    protected int sleepIntervalInMillis() {
        return 10_000;
    }

    /**
     * Cancels future initializations and registers a callback to be called if the initialization is happening and
     * succeeds. If the initialization has already successfully completed, runs the cleanup task synchronously.
     *
     * If the instance is closeable, it's recommended that the this method is invoked in a close call, and the callback
     * contains a call to the instance's close method.
     */
    protected final void cancelInitialization(Runnable handler) {
        if (state.initToCancelWithCleanupTask(handler) == AsyncInitializationState.State.DONE) {
            handler.run();
        }
    }

    protected final void checkInitialized() {
        if (!isInitialized()) {
            throw new NotInitializedException(getInitializingClassName());
        }
    }

    private void tryInitializeInternal() {
        tryInitialize();
        if (state.initToDone() == AsyncInitializationState.State.CANCELLED) {
            state.performCleanupTask();
        }
        singleThreadedExecutor.shutdown();
    }

    // Not final for tests.
    public boolean isInitialized() {
        return state.isDone();
    }

    /**
     * Override this method if there's anything to be cleaned up on initialization failure.
     * Default implementation is no-op.
     */
    protected void cleanUpOnInitFailure() {
        // no-op
    }

    /**
     * Override this method with the calls to initialize an object that may fail.
     * This method will be retried if any exception is thrown on its execution.
     * If there's any follow up action to clean any state left by a previous initialization failure, see
     * {@link AsyncInitializer#cleanUpOnInitFailure}.
     */
    protected abstract void tryInitialize();

    /**
     * This method should contain the name of the initializing class. It's used for logging and exception propagation.
     */
    protected abstract String getInitializingClassName();
}
