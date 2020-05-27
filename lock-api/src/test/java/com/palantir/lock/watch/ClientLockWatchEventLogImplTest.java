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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockToken;

@RunWith(MockitoJUnitRunner.class)
public final class ClientLockWatchEventLogImplTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final IdentifiedVersion VERSION_0 = IdentifiedVersion.of(LEADER, 0L);
    private static final IdentifiedVersion VERSION_1 = IdentifiedVersion.of(LEADER, 17L);
    private static final IdentifiedVersion VERSION_2 = IdentifiedVersion.of(LEADER, 38L);
    private static final LockWatchEvent EVENT_1 =
            LockEvent.builder(ImmutableSet.of(), LockToken.of(UUID.randomUUID())).build(17L);
    private static final LockWatchEvent EVENT_2 =
            LockEvent.builder(ImmutableSet.of(), LockToken.of(UUID.randomUUID())).build(38L);
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(VERSION_1.id(), VERSION_1.version(), ImmutableSet.of(), ImmutableSet.of());
    private static final LockWatchStateUpdate.Success SUCCESS =
            LockWatchStateUpdate.success(VERSION_2.id(), VERSION_2.version(), ImmutableList.of(EVENT_1, EVENT_2));
    private static final LockWatchStateUpdate.Success SUCCESS_EMPTY =
            LockWatchStateUpdate.success(VERSION_2.id(), VERSION_2.version(), ImmutableList.of());
    private static final LockWatchStateUpdate.Failed FAILED =
            LockWatchStateUpdate.failed(LEADER);
    private static final Map<Long, IdentifiedVersion> TIMESTAMPS =
            ImmutableMap.of(1L, VERSION_1, 2L, VERSION_1, 3L, VERSION_2);

    @Mock
    private ClientLockWatchSnapshotUpdater snapshotUpdater;
    private ClientLockWatchEventLog eventLog;

    @Before
    public void before() {
        eventLog = ClientLockWatchEventLogImpl.create(snapshotUpdater);
    }

    @Test
    public void snapshotUpdateResetsSnapshotter() {
        eventLog.processUpdate(SNAPSHOT, Optional.empty());
        verify(snapshotUpdater).resetWithSnapshot(SNAPSHOT);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_1);
    }

    @Test
    public void failedUpdateResetsSnapshotter() {
        eventLog.processUpdate(FAILED, Optional.empty());
        verify(snapshotUpdater).reset();
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void getEventsForTransactionsReturnsRequiredEventsOnly() {
        eventLog.processUpdate(SNAPSHOT, Optional.empty());
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_1);
        eventLog.processUpdate(SUCCESS, Optional.empty());
        List<LockWatchEvent> events = eventLog.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION_1))
                .accept(SuccessVisitor.INSTANCE).events();
        assertThat(events).containsExactly(EVENT_2);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_2);
    }

    @Test
    public void oldEventsAreDeletedWhenEarliestVersionIsProvided() {
        eventLog.processUpdate(SNAPSHOT, Optional.empty());
        eventLog.processUpdate(SUCCESS, Optional.empty());
        List<LockWatchEvent> events = eventLog.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION_1))
                .accept(SuccessVisitor.INSTANCE).events();
        assertThat(events).containsExactly(EVENT_1, EVENT_2);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_2);

        when(snapshotUpdater.getSnapshot(VERSION_2)).thenReturn(LockWatchStateUpdate.snapshot(
                        VERSION_2.id(),
                        VERSION_2.version(),
                        ImmutableSet.of(),
                        ImmutableSet.of()));
        eventLog.processUpdate(SUCCESS_EMPTY, Optional.of(VERSION_2));
        eventLog.getEventsForTransactions(TIMESTAMPS, Optional.of(VERSION_1));
        verify(snapshotUpdater).getSnapshot(VERSION_2);
    }

    enum SuccessVisitor implements TransactionsLockWatchEvents.Visitor<TransactionsLockWatchEvents.Events> {
        INSTANCE;

        @Override
        public TransactionsLockWatchEvents.Events visit(TransactionsLockWatchEvents.Events success) {
            return success;
        }

        @Override
        public TransactionsLockWatchEvents.Events visit(TransactionsLockWatchEvents.ForcedSnapshot failure) {
            fail("Expected success outcome");
            return null;
        }
    }

    enum SnapshotVisitor implements TransactionsLockWatchEvents.Visitor<TransactionsLockWatchEvents.ForcedSnapshot> {
        INSTANCE;

        @Override
        public TransactionsLockWatchEvents.ForcedSnapshot visit(TransactionsLockWatchEvents.Events success) {
            fail("Expected snapshot outcome");
            return null;
        }

        @Override
        public TransactionsLockWatchEvents.ForcedSnapshot visit(TransactionsLockWatchEvents.ForcedSnapshot failure) {
            return failure;
        }
    }
}
