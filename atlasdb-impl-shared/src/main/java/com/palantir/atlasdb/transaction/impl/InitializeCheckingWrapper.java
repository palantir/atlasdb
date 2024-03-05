/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.transaction.api.AutoDelegate_TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.LockService;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class InitializeCheckingWrapper implements AutoDelegate_TransactionManager {
    private static final SafeLogger log = SafeLoggerFactory.get(InitializeCheckingWrapper.class);

    private final TransactionManager txManager;
    private final Supplier<Boolean> initializationPrerequisite;
    private final Callback<TransactionManager> callback;

    private State status = State.INITIALIZING;
    private Throwable callbackThrowable = null;

    private final ScheduledExecutorService executorService;

    public InitializeCheckingWrapper(
            TransactionManager manager,
            Supplier<Boolean> initializationPrerequisite,
            Callback<TransactionManager> callback,
            ScheduledExecutorService initializer) {
        this.txManager = manager;
        this.initializationPrerequisite = initializationPrerequisite;
        this.callback = callback;
        this.executorService = initializer;
        scheduleInitializationCheckAndCallback();
    }

    @Override
    public TransactionManager delegate() {
        assertOpen();
        if (!isInitialized()) {
            throw new NotInitializedException("TransactionManager");
        }
        return txManager;
    }

    @Override
    public boolean isInitialized() {
        assertOpen();
        return status == State.READY && isInitializedInternal();
    }

    @Override
    public LockService getLockService() {
        assertOpen();
        return txManager.getLockService();
    }

    @Override
    public void registerClosingCallback(Runnable closingCallback) {
        assertOpen();
        txManager.registerClosingCallback(closingCallback);
    }

    @Override
    public void close() {
        closeInternal(State.CLOSED);
    }

    @VisibleForTesting
    boolean isClosedByClose() {
        return status == State.CLOSED;
    }

    @VisibleForTesting
    boolean isClosedByCallbackFailure() {
        return status == State.CLOSED_BY_CALLBACK_FAILURE;
    }

    private void assertOpen() {
        if (status == State.CLOSED) {
            throw new SafeIllegalStateException("Operations cannot be performed on closed TransactionManager.");
        }
        if (status == State.CLOSED_BY_CALLBACK_FAILURE) {
            throw new SafeIllegalStateException(
                    "Operations cannot be performed on closed TransactionManager."
                            + " Closed due to a callback failure.",
                    callbackThrowable);
        }
    }

    private void runCallbackIfInitializedOrScheduleForLater(Runnable callbackTask) {
        if (isInitializedInternal()) {
            callbackTask.run();
        } else {
            scheduleInitializationCheckAndCallback();
        }
    }

    private void scheduleInitializationCheckAndCallback() {
        executorService.schedule(
                () -> {
                    if (status != State.INITIALIZING) {
                        return;
                    }
                    runCallbackIfInitializedOrScheduleForLater(this::runCallbackWithRetry);
                },
                1_000,
                TimeUnit.MILLISECONDS);
    }

    private boolean isInitializedInternal() {
        // Note that the PersistentLockService is also initialized asynchronously as part of
        // TransactionManagers.create; however, this is not required for the TransactionManager to fulfil
        // requests (note that it is not accessible from any TransactionManager implementation), so we omit
        // checking here whether it is initialized.
        return txManager.getKeyValueService().isInitialized()
                && txManager.getTimelockService().isInitialized()
                && txManager.getTimestampService().isInitialized()
                && txManager.getCleaner().isInitialized()
                && initializationPrerequisite.get();
    }

    private void runCallbackWithRetry() {
        try {
            callback.runWithRetry(txManager);
            changeStateToReady();
        } catch (Throwable e) {
            changeStateToCallbackFailure(e);
        }
    }

    private void changeStateToReady() {
        if (checkAndSetStatus(ImmutableSet.of(State.INITIALIZING), State.READY)) {
            executorService.shutdown();
        }
    }

    private void changeStateToCallbackFailure(Throwable th) {
        log.error(
                "Callback failed and was not able to perform its cleanup task. " + "Closing the TransactionManager.",
                th);
        callbackThrowable = th;
        closeInternal(State.CLOSED_BY_CALLBACK_FAILURE);
    }

    private void closeInternal(State newStatus) {
        if (checkAndSetStatus(ImmutableSet.of(State.INITIALIZING, State.READY), newStatus)) {
            callback.blockUntilSafeToShutdown();
            executorService.shutdown();
            txManager.close();
        }
    }

    private synchronized boolean checkAndSetStatus(Set<State> expected, State desired) {
        if (expected.contains(status)) {
            status = desired;
            return true;
        }
        return false;
    }

    private enum State {
        INITIALIZING,
        READY,
        CLOSED,
        CLOSED_BY_CALLBACK_FAILURE
    }
}
