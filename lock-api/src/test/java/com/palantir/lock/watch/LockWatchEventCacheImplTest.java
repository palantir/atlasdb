/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventCacheImplTest {
    private static IdentifiedVersion VERSION = IdentifiedVersion.of(UUID.randomUUID(), 17L);
    private static LockWatchStateUpdate.Success SUCCESS =
            LockWatchStateUpdate.success(VERSION.id(), VERSION.version(), ImmutableList.of());
    private static LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(UUID.randomUUID(), 0L, ImmutableSet.of(), ImmutableSet.of());
    private static LockWatchStateUpdate.Failed FAILED = LockWatchStateUpdate.failed(UUID.randomUUID());


    @Mock
    private ClientLockWatchEventLog eventLog;

    private LockWatchEventCacheImpl eventCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(eventLog);
    }

    @Test
    public void processUpdatePassesThroughToEventLog() {
        eventCache.processUpdate(SUCCESS);
        verify(eventLog).processUpdate(eq(SUCCESS), any());
    }

    @Test
    public void processStartTransactionUpdatePassesThroughToEventLog() {
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);
        verify(eventLog).processUpdate(eq(SUCCESS), any());
    }



}
