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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.monitoring.TimestampTrackerImpl;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.InMemoryTimestampService;

public class SerializableTransactionManagerTest {

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private TimelockService mockTimelockService = mock(TimelockService.class);
    private Cleaner mockCleaner = mock(Cleaner.class);
    private AsyncInitializer mockInitializer = mock(AsyncInitializer.class);

    private SerializableTransactionManager manager;

    @Before
    public void setUp() {
        manager = SerializableTransactionManagerImpl.create(
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
                true, // initializeAsync
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE,
                SweepQueueWriter.NO_OP);

        when(mockKvs.isInitialized()).thenReturn(true);
        when(mockTimelockService.isInitialized()).thenReturn(true);
        when(mockCleaner.isInitialized()).thenReturn(true);
        when(mockInitializer.isInitialized()).thenReturn(true);
    }

    @Test
    public void isInitializedWhenPrerequisitesAreInitialized() {
        assertTrue(manager.isInitialized());
    }

    @Test
    public void isNotInitializedWhenKvsIsNotInitialized() {
        when(mockKvs.isInitialized()).thenReturn(false);
        assertFalse(manager.isInitialized());
    }

    @Test
    public void isNotInitializedWhenTimelockIsNotInitialized() {
        when(mockTimelockService.isInitialized()).thenReturn(false);
        assertFalse(manager.isInitialized());
    }

    @Test
    public void isNotInitializedWhenCleanerIsNotInitialized() {
        when(mockCleaner.isInitialized()).thenReturn(false);
        assertFalse(manager.isInitialized());
    }

    @Test
    public void isNotInitializedWhenInitializerIsNotInitialized() {
        when(mockInitializer.isInitialized()).thenReturn(false);
        assertFalse(manager.isInitialized());
    }

    // Edge case: if for some reason we create a SerializableTransactionManager with initializeAsync set to false, we
    // should initialise it synchronously, even if some of its component parts are initialised asynchronously.
    // If we somehow manage to survive doing this with no exception, even though the KVS (for example) is not
    // initialised, then isInitialized should return true.
    //
    // BLAB: Synchronously initialised objects don't care if their constituent parts are initialised asynchronously.
    @Test
    public void synchronouslyInitializedManagerIsInitializedEvenIfKvsIsNot() {
        SerializableTransactionManager theManager = SerializableTransactionManagerImpl.create(
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
                () -> 1L, // lockAcquireTimeoutMs
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                false, // initializeAsync
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE,
                SweepQueueWriter.NO_OP);

        when(mockKvs.isInitialized()).thenReturn(false);
        when(mockTimelockService.isInitialized()).thenReturn(false);
        when(mockCleaner.isInitialized()).thenReturn(false);
        when(mockInitializer.isInitialized()).thenReturn(false);

        assertTrue(theManager.isInitialized());
    }

    @Test
    public void closesKeyValueServiceOnClose() {
        manager.close();
        verify(mockKvs, times(1)).close();
    }

    @Test
    public void closesCleanerOnClose() {
        manager.close();
        verify(mockCleaner, times(1)).close();
    }

    @Test
    public void closesCloseableLockServiceOnClosingTransactionManager() throws IOException {
        CloseableLockService closeableLockService = mock(CloseableLockService.class);
        SerializableTransactionManager legacyTimelockTxnManager = new SerializableTransactionManagerImpl(
                mockKvs,
                new LegacyTimelockService(new InMemoryTimestampService(), closeableLockService,
                        LockClient.of("lock")),
                closeableLockService,
                null,
                null,
                null,
                null,
                mockCleaner,
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                TimestampTrackerImpl.createNoOpTracker(),
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                () -> AtlasDbConstants.DEFAULT_TIMESTAMP_CACHE_SIZE,
                SweepQueueWriter.NO_OP);
        legacyTimelockTxnManager.close();
        verify(closeableLockService, times(1)).close();
    }

    @Test
    public void cannotRegisterNullCallback() {
        assertThatThrownBy(() -> manager.registerClosingCallback(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void invokesCallbackOnClose() {
        Runnable callback = mock(Runnable.class);

        manager.registerClosingCallback(callback);
        verify(callback, never()).run();
        manager.close();
        verify(callback).run();
    }

    @Test
    public void invokesCallbacksInReverseOrderOfRegistration() {
        Runnable callback1 = mock(Runnable.class);
        Runnable callback2 = mock(Runnable.class);
        InOrder inOrder = inOrder(callback1, callback2);

        manager.registerClosingCallback(callback1);
        manager.registerClosingCallback(callback2);
        manager.close();
        inOrder.verify(callback2).run();
        inOrder.verify(callback1).run();
    }
}
