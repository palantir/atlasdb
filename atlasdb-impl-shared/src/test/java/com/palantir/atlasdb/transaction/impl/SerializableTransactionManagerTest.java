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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Test;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.lock.v2.TimelockService;

public class SerializableTransactionManagerTest {

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private TimelockService mockTimelockService = mock(TimelockService.class);
    private Cleaner mockCleaner = mock(Cleaner.class);
    private AsyncInitializer mockInitializer = mock(AsyncInitializer.class);
    private Runnable mockCallback = mock(Runnable.class);

    private SerializableTransactionManager manager;

    @Before
    public void setUp() {
        when(mockKvs.isInitialized()).thenReturn(false);
        manager = getManagerWithCallback(true, mockCallback);

        when(mockTimelockService.isInitialized()).thenReturn(true);
        when(mockCleaner.isInitialized()).thenReturn(true);
        when(mockInitializer.isInitialized()).thenReturn(true);
    }

    @Test
    public void isInitializedWhenPrerequisitesAreInitialized() {
        when(mockKvs.isInitialized()).thenReturn(true);
        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized());
        verify(mockCallback).run();
    }

    @Test
    public void closingPreventsCallback() {
        manager.close();
        when(mockKvs.isInitialized()).thenReturn(true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).run();
    }

    @Test
    public void isNotInitializedWhenKvsIsNotInitialized() {
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).run();
    }

    @Test
    public void isNotInitializedWhenTimelockIsNotInitialized() {
        when(mockTimelockService.isInitialized()).thenReturn(false);
        when(mockKvs.isInitialized()).thenReturn(true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).run();
    }

    @Test
    public void isNotInitializedWhenCleanerIsNotInitialized() {
        when(mockCleaner.isInitialized()).thenReturn(false);
        when(mockKvs.isInitialized()).thenReturn(true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).run();
    }

    @Test
    public void isNotInitializedWhenInitializerIsNotInitialized() {
        when(mockInitializer.isInitialized()).thenReturn(false);
        when(mockKvs.isInitialized()).thenReturn(true);
        assertNotInitializedWithinTwoSeconds();
        verify(mockCallback, never()).run();
    }

    @Test
    public void callbackThrowingPreventsInitializationAndCloses() {
        when(mockKvs.isInitialized()).thenReturn(true);
        AtomicBoolean invoked = new AtomicBoolean(false);
        Runnable throwingCallback = () -> {
            invoked.set(true);
            throw new RuntimeException();
        };
        manager = getManagerWithCallback(true, throwingCallback);
        assertNotInitializedWithinTwoSeconds();
        assertTrue(manager.isClosed.get());
        assertTrue(invoked.get());
    }

    // Edge case: if for some reason we create a SerializableTransactionManager with initializeAsync set to false, we
    // should initialise it synchronously, even if some of its component parts are initialised asynchronously.
    // If we somehow manage to survive doing this with no exception, even though the KVS (for example) is not
    // initialised, then isInitialized should return true.
    //
    // BLAB: Synchronously initialised objects don't care if their constituent parts are initialised asynchronously.
    @Test
    public void synchronouslyInitializedManagerIsInitializedEvenIfNothingElseIs() {
        when(mockKvs.isInitialized()).thenReturn(false);
        when(mockTimelockService.isInitialized()).thenReturn(false);
        when(mockCleaner.isInitialized()).thenReturn(false);
        when(mockInitializer.isInitialized()).thenReturn(false);

        SerializableTransactionManager theManager = getManagerWithCallback(false, mockCallback);

        assertTrue(theManager.isInitialized());
        verify(mockCallback, never()).run();
    }

    @Test
    public void callbackRunsAfterPreconditionsAreMetAndBlocksInitializationUntilDone() {
        BlockingRunnable blockingCallback = new BlockingRunnable();
        SerializableTransactionManager managerWithBlockingCallback = getManagerWithCallback(true, blockingCallback);

        assertNotInitializedWithinTwoSeconds();
        assertFalse(blockingCallback.wasInvoked());

        when(mockKvs.isInitialized()).thenReturn(true);

        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> blockingCallback.wasInvoked());
        assertFalse(managerWithBlockingCallback.isInitialized());

        blockingCallback.stopBlocking();

        Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> managerWithBlockingCallback.isInitialized());
    }

    private SerializableTransactionManager getManagerWithCallback(boolean initializeAsync, Runnable callBack) {
        return SerializableTransactionManager.create(
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
                SweepQueueWriter.NO_OP,
                callBack);
    }

    private void assertNotInitializedWithinTwoSeconds() {
        Assertions.assertThatThrownBy(() ->
                Awaitility.waitAtMost(2L, TimeUnit.SECONDS).until(() -> manager.isInitialized()))
                .isInstanceOf(ConditionTimeoutException.class);
    }

    private static class BlockingRunnable implements Runnable {
        private volatile boolean invoked = false;
        private volatile boolean block = true;

        @Override
        public void run() {
            invoked = true;
            while (block) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    fail();
                }
            }
        }

        boolean wasInvoked() {
            return invoked;
        }

        void stopBlocking() {
            block = false;
        }
    }
}
