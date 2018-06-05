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

package com.palantir.atlasdb.transaction.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Test;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.ClusterAvailabilityStatus;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.exception.NotInitializedException;
import com.palantir.lock.v2.TimelockService;

public class SerializableTransactionManagerTest {

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private TimelockService mockTimelockService = mock(TimelockService.class);
    private Cleaner mockCleaner = mock(Cleaner.class);
    private AsyncInitializer mockInitializer = mock(AsyncInitializer.class);
    private Callback<SerializableTransactionManager> mockCallback = mock(Callback.class);

    private SerializableTransactionManager manager;

    @Before
    public void setUp() {
        nothingInitialized();
        manager = getManagerWithCallback(true, mockCallback);
        when(mockKvs.getClusterAvailabilityStatus()).thenReturn(ClusterAvailabilityStatus.ALL_AVAILABLE);
    }

    @Test
    public void uninitializedTransactionManagerThrowsNotInitializedException() {
        assertNotInitializedWithinTwoSeconds();
        Assertions.assertThatThrownBy(() -> manager.runTaskWithRetry($  -> null))
                .isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void isInitializedAndCallbackHasRunWhenPrerequisitesAreInitialized() {
        everythingInitialized();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());
        verify(mockCallback).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void switchBackToUninitializedWhenPrerequisitesSwitchToUninitialized() {
        everythingInitialized();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());

        nothingInitialized();
        assertNotInitializedWithinTwoSeconds();
        Assertions.assertThatThrownBy(() -> manager.runTaskWithRetry($  -> null))
                .isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void callbackRunsOnlyOnceAsInitializationStatusChanges() {
        everythingInitialized();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());

        nothingInitialized();
        assertNotInitializedWithinTwoSeconds();

        everythingInitialized();
        assertTrue(manager.isInitialized());
        verify(mockCallback).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void closingPreventsInitializationAndCallback() {
        manager.close();
        everythingInitialized();
        assertNotInitializedWithinTwoSecondsBecauseClosed();
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
        assertTrue(((SerializableTransactionManager.InitializeCheckingWrapper) manager).isClosedByClose());
    }

    @Test
    public void isNotInitializedWhenKvsIsNotInitialized() {
        setInitializationStatus(false, true, true, true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenTimelockIsNotInitialized() {
        setInitializationStatus(true, false, true, true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenCleanerIsNotInitialized() {
        setInitializationStatus(true, true, false, true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void isNotInitializedWhenInitializerIsNotInitialized() {
        setInitializationStatus(true, true, true, false);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).runWithRetry(any(SerializableTransactionManager.class));
    }

    @Test
    public void exceptionInCleanupClosesTransactionManager() {
        RuntimeException cause = new RuntimeException("VALID REASON");
        doThrow(cause).when(mockCallback).runWithRetry(any(SerializableTransactionManager.class));
        everythingInitialized();

        assertNotInitializedWithinTwoSecondsBecauseClosed();
        assertTrue(((SerializableTransactionManager.InitializeCheckingWrapper) manager).isClosedByCallbackFailure());
        Assertions.assertThatThrownBy(() -> manager.runTaskWithRetry($  -> null))
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
        manager = getManagerWithCallback(false, mockCallback);
        assertTrue(manager.isInitialized());
        verify(mockCallback).runWithRetry(manager);
    }

    @Test
    public void callbackRunsAfterPreconditionsAreMet() {
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        manager = getManagerWithCallback(true, blockingCallback);

        assertNotInitializedWithinTwoSeconds();
        assertFalse(blockingCallback.wasInvoked());

        everythingInitialized();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> blockingCallback.wasInvoked());
    }

    @Test
    public void callbackBlocksInitializationUntilDone() {
        everythingInitialized();
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        manager = getManagerWithCallback(true, blockingCallback);

        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> blockingCallback.wasInvoked());
        assertFalse(manager.isInitialized());

        blockingCallback.stopBlocking();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());
    }

    @Test
    public void callbackCanCallTmMethodsEvenThoughTmStillThrows() {
        everythingInitialized();
        ClusterAvailabilityStatusBlockingCallback blockingCallback = new ClusterAvailabilityStatusBlockingCallback();
        manager = getManagerWithCallback(true, blockingCallback);

        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> blockingCallback.wasInvoked());
        verify(mockKvs, atLeast(1)).getClusterAvailabilityStatus();
        Assertions.assertThatThrownBy(() -> manager.getKeyValueServiceStatus())
                .isInstanceOf(NotInitializedException.class);

        blockingCallback.stopBlocking();
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());
        manager.getKeyValueServiceStatus();
    }

    private SerializableTransactionManager getManagerWithCallback(boolean initializeAsync,
            Callback<SerializableTransactionManager> callBack) {
        return SerializableTransactionManager.create(
                MetricsManagers.createForTests(),
                mockKvs,
                mockTimelockService,
                null, // lockService
                null, // transactionService
                () -> null, // constraintMode
                null, // conflictDetectionManager
                null, // sweepStrategyManager
                mockCleaner,
                mockInitializer::isInitialized,
                false, // allowHiddenTableAccess
                () -> 1L, // lockAcquireTimeout
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                initializeAsync,
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE,
                MultiTableSweepQueueWriter.NO_OP,
                callBack);
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

    private void assertNotInitializedWithinTwoSeconds() {
        Assertions.assertThatThrownBy(() ->
                Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized()))
                .isInstanceOf(ConditionTimeoutException.class);
    }

    private void assertNotInitializedWithinTwoSecondsBecauseClosed() {
        Assertions.assertThatThrownBy(() ->
                Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized()))
                .isInstanceOf(IllegalStateException.class);
    }

    private static class ClusterAvailabilityStatusBlockingCallback extends Callback<SerializableTransactionManager> {
        private volatile boolean invoked = false;
        private volatile boolean block = true;

        boolean wasInvoked() {
            return invoked;
        }

        void stopBlocking() {
            block = false;
        }

        @Override
        public void init(SerializableTransactionManager transactionManager) {
            invoked = true;
            transactionManager.getKeyValueServiceStatus();
            if (block) {
                throw new RuntimeException();
            }
        }

        @Override
        public void cleanup(SerializableTransactionManager transactionManager, Throwable initException) {
            try {
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                fail();
            }
        }
    }
}
