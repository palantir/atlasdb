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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.watch.NoOpLockWatchManager;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.KeyValueServiceStatus;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.timestamp.TimestampManagementService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class SerializableTransactionManagerTest {
    private static final long THREE = 3L;

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private TimelockService mockTimelockService = mock(TimelockService.class);
    private TimestampManagementService mockTimestampManagementService = mock(TimestampManagementService.class);
    private Cleaner mockCleaner = mock(Cleaner.class);
    private AsyncInitializer mockInitializer = mock(AsyncInitializer.class);
    private Callback<TransactionManager> mockCallback = mock(Callback.class);

    private DeterministicScheduler executorService;
    private TransactionManager manager;

    @Before
    public void setUp() {
        nothingInitialized();
        executorService = new DeterministicSchedulerWithShutdownFlag();
        manager = getManagerWithCallback(true, mockCallback, executorService);
        when(mockKvs.getClusterAvailabilityStatus()).thenReturn(ClusterAvailabilityStatus.ALL_AVAILABLE);
    }

    @Test
    public void transactionManagerCannotInitializeWhilePrerequisitesAreFalse() {
        assertFalse(manager.isInitialized());
        tickInitializingThread();
        assertFalse(manager.isInitialized());
        tickInitializingThread();
        assertFalse(manager.isInitialized());
    }

    @Test
    public void uninitializedTransactionManagerThrowsNotInitializedException() {
        assertThatThrownBy(() -> manager.runTaskWithRetry(ignore -> null)).isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void isInitializedAndCallbackHasRunWhenPrerequisitesAreInitialized() {
        everythingInitialized();
        tickInitializingThread();
        assertTrue(manager.isInitialized());
        verify(mockCallback, times(1)).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void initializingExecutorShutsDownWhenInitialized() {
        everythingInitialized();
        tickInitializingThread();
        assertTrue(manager.isInitialized());

        assertTrue(executorService.isShutdown());
    }

    @Test
    public void switchBackToUninitializedImmediatelyWhenPrerequisitesBecomeFalse() {
        everythingInitialized();
        tickInitializingThread();
        assertTrue(manager.isInitialized());

        nothingInitialized();
        assertFalse(manager.isInitialized());
        assertThatThrownBy(() -> manager.runTaskWithRetry(ignore -> null)).isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void callbackRunsOnlyOnceAsInitializationStatusChanges() {
        everythingInitialized();
        tickInitializingThread();
        assertTrue(manager.isInitialized());

        nothingInitialized();
        assertFalse(manager.isInitialized());

        everythingInitialized();
        assertTrue(manager.isInitialized());

        verify(mockCallback, times(1)).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void closeShutsDownInitializingExecutorAndClosesTransactionManager() {
        manager.close();

        assertTrue(executorService.isShutdown());
        assertThatThrownBy(() -> manager.runTaskWithRetry(ignore -> null)).isInstanceOf(IllegalStateException.class);
        assertTrue(((SerializableTransactionManager.InitializeCheckingWrapper) manager).isClosedByClose());
    }

    @Test
    public void closePreventsInitializationAndCallbacksEvenIfExecutorStillTicks() {
        manager.close();
        everythingInitialized();
        tickInitializingThread();

        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
        assertThatThrownBy(() -> manager.runTaskWithRetry(ignore -> null)).isInstanceOf(IllegalStateException.class);
        assertTrue(((SerializableTransactionManager.InitializeCheckingWrapper) manager).isClosedByClose());
    }

    @Test
    public void isNotInitializedWhenKvsIsNotInitialized() {
        setInitializationStatus(false, true, true, true);
        tickInitializingThread();
        assertFalse(manager.isInitialized());
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenTimelockIsNotInitialized() {
        setInitializationStatus(true, false, true, true);
        tickInitializingThread();
        assertFalse(manager.isInitialized());
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenCleanerIsNotInitialized() {
        setInitializationStatus(true, true, false, true);
        tickInitializingThread();
        assertFalse(manager.isInitialized());
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenInitializerIsNotInitialized() {
        setInitializationStatus(true, true, true, false);
        tickInitializingThread();
        assertFalse(manager.isInitialized());
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void exceptionInCleanupClosesTransactionManager() {
        RuntimeException cause = new RuntimeException("VALID REASON");
        doThrow(cause).when(mockCallback).runWithRetry(any(SerializableTransactionManager.class));
        everythingInitialized();
        tickInitializingThread();

        assertTrue(((SerializableTransactionManager.InitializeCheckingWrapper) manager).isClosedByCallbackFailure());
        assertThatThrownBy(() -> manager.runTaskWithRetry($  -> null))
                .isInstanceOf(IllegalStateException.class)
                .hasCause(cause);
    }

    // Edge case: if for some reason we create a SerializableTransactionManager with initializeAsync set to false, we
    // should initialise it synchronously, even if some of its component parts are initialised asynchronously.
    // If we somehow manage to survive doing this with no exception, even though the KVS (for example) is not
    // initialised, then isInitialized should return true.
    //
    // BLAB: Synchronously initialised objects don't care if their constituent parts are initialised asynchronously.
    @Test
    public void synchronouslyInitializedManagerIsInitializedEvenIfNothingElseIs() {
        manager = getManagerWithCallback(false, mockCallback, executorService);
        assertTrue(manager.isInitialized());
        verify(mockCallback).runWithRetry(manager);
    }

    @Test
    public void callbackRunsAfterPreconditionsAreMet() {
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        blockingCallback.stopBlocking();
        manager = getManagerWithCallback(true, blockingCallback, executorService);

        tickInitializingThread();
        assertFalse(manager.isInitialized());
        assertFalse(blockingCallback.wasInvoked());

        everythingInitialized();
        tickInitializingThread();
        assertTrue(blockingCallback.wasInvoked());
    }

    @Test
    public void callbackBlocksInitializationUntilDone() {
        everythingInitialized();
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        manager = getManagerWithCallback(true, blockingCallback, executorService);

        ExecutorService tickerThread = PTExecutors.newSingleThreadExecutor(true);
        tickerThread.execute(() -> executorService.tick(1000, TimeUnit.MILLISECONDS));

        Awaitility.waitAtMost(THREE, TimeUnit.SECONDS).until(blockingCallback::wasInvoked);
        assertFalse(manager.isInitialized());

        blockingCallback.stopBlocking();
        Awaitility.waitAtMost(THREE, TimeUnit.SECONDS).until(manager::isInitialized);
        tickerThread.shutdown();
    }

    @Test
    public void callbackCanCallTmMethodsEvenThoughTmStillThrows() {
        everythingInitialized();
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        manager = getManagerWithCallback(true, blockingCallback, executorService);

        ExecutorService tickerThread = PTExecutors.newSingleThreadExecutor(true);
        tickerThread.execute(() -> executorService.tick(1000, TimeUnit.MILLISECONDS));

        Awaitility.waitAtMost(THREE, TimeUnit.SECONDS).until(blockingCallback::wasInvoked);
        verify(mockKvs, atLeast(1)).getClusterAvailabilityStatus();
        assertThatThrownBy(manager::getKeyValueServiceStatus).isInstanceOf(NotInitializedException.class);

        blockingCallback.stopBlocking();
        Awaitility.waitAtMost(THREE, TimeUnit.SECONDS).until(manager::isInitialized);
        assertThat(manager.getKeyValueServiceStatus()).isEqualTo(KeyValueServiceStatus.HEALTHY_ALL_OPERATIONS);
        tickerThread.shutdown();
    }

    private TransactionManager getManagerWithCallback(boolean initializeAsync,
            Callback<TransactionManager> callBack, ScheduledExecutorService executor) {
        return SerializableTransactionManager.create(
                MetricsManagers.createForTests(),
                mockKvs,
                mockTimelockService,
                NoOpLockWatchManager.INSTANCE,
                NoOpLockWatchEventCache.INSTANCE,
                mockTimestampManagementService,
                null, // lockService
                mock(TransactionService.class),
                () -> null, // constraintMode
                null, // conflictDetectionManager
                null, // sweepStrategyManager
                mockCleaner,
                mockInitializer::isInitialized,
                false, // allowHiddenTableAccess
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                initializeAsync,
                DefaultTimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                callBack,
                executor,
                true,
                () -> ImmutableTransactionConfig.builder().build(),
                ConflictTracer.NO_OP);
    }

    private void nothingInitialized() {
        setInitializationStatus(false, false, false, false);
    }

    private void everythingInitialized() {
        setInitializationStatus(true, true, true, true);
    }

    private void setInitializationStatus(boolean kvs, boolean timelock, boolean cleaner, boolean initializer) {
        when(mockKvs.isInitialized()).thenReturn(kvs);
        when(mockTimelockService.isInitialized()).thenReturn(timelock);
        when(mockCleaner.isInitialized()).thenReturn(cleaner);
        when(mockInitializer.isInitialized()).thenReturn(initializer);
    }

    private void tickInitializingThread() {
        executorService.tick(1000, TimeUnit.MILLISECONDS);
    }

    private static class ClusterAvailabilityStatusBlockingCallback extends Callback<TransactionManager> {
        private volatile boolean successfullyInvoked = false;
        private volatile boolean block = true;

        boolean wasInvoked() {
            return successfullyInvoked;
        }

        void stopBlocking() {
            block = false;
        }

        @Override
        public void init(TransactionManager transactionManager) {
            successfullyInvoked =
                    transactionManager.getKeyValueServiceStatus() == KeyValueServiceStatus.HEALTHY_ALL_OPERATIONS;
            while (block) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class DeterministicSchedulerWithShutdownFlag extends DeterministicScheduler {
        private boolean hasShutdown = false;

        @Override
        public boolean isShutdown() {
            return hasShutdown;
        }

        @Override
        public void shutdown() {
            hasShutdown = true;
        }
    }
}
