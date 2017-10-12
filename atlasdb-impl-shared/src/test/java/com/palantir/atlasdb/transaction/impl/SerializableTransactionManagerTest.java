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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.lock.v2.TimelockService;

public class SerializableTransactionManagerTest {

    private KeyValueService mockKvs = mock(KeyValueService.class);
    private TimelockService mockTimelockService = mock(TimelockService.class);
    private Cleaner mockCleaner = mock(Cleaner.class);
    private AsyncInitializer mockInitializer = mock(AsyncInitializer.class);

    private SerializableTransactionManager manager;

    @Before
    public void setUp() {
        manager = SerializableTransactionManager.create(
                mockKvs,
                mockTimelockService,
                null, // lockService
                null, // transactionService
                () -> null, // constraintMode
                null, // conflictDetectionManager
                null, // sweepStrategyManager
                mockCleaner,
                mockInitializer,
                false, // allowHiddenTableAccess
                () -> 1L, // lockAcquireTimeoutMs
                1, // concurrentGetRangesThreadPoolSize
                true); // initializeAsync

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
}
