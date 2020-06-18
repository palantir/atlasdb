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

package com.palantir.atlasdb.keyvalue.api.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventLogImplTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final IdentifiedVersion VERSION_1 = IdentifiedVersion.of(LEADER, 17L);
    private static final IdentifiedVersion VERSION_2 = IdentifiedVersion.of(LEADER, 25L);
    private static final LockWatchEvent EVENT_1 =
            LockEvent.builder(ImmutableSet.of(), LockToken.of(UUID.randomUUID())).build(17L);
    private static final LockWatchEvent EVENT_2 =
            LockEvent.builder(ImmutableSet.of(), LockToken.of(UUID.randomUUID())).build(25L);
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(VERSION_1.id(), VERSION_1.version(), ImmutableSet.of(), ImmutableSet.of());
    private static final LockWatchStateUpdate.Success SUCCESS =
            LockWatchStateUpdate.success(VERSION_2.id(), VERSION_2.version(), ImmutableList.of(EVENT_1, EVENT_2));
    private static final LockWatchStateUpdate.Failed FAILED =
            LockWatchStateUpdate.failed(LEADER);

    @Mock
    private ClientLockWatchSnapshot snapshot;

    private LockWatchEventLog eventLog;

    @Before
    public void before() {
//        eventLog = LockWatchEventLogImpl.create(snapshot);
    }

    @Test
    public void failedUpdateResetsSnapshot() {
        eventLog.processUpdate(FAILED);
        verify(snapshot).reset();
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void leaderChangeResetsSnapshot() {
        eventLog.processUpdate(SNAPSHOT);
        verify(snapshot).resetWithSnapshot(SNAPSHOT);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_1);

        eventLog.processUpdate(
                LockWatchStateUpdate.success(UUID.randomUUID(), 1L, ImmutableList.of())
        );
        verify(snapshot).reset();
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void getEventsForTransactionsReturnsRequiredEventsOnly() {
        eventLog.processUpdate(SNAPSHOT);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_1);
        eventLog.processUpdate(SUCCESS);
        List<LockWatchEvent> events =
                eventLog.getEventsBetweenVersions(Optional.of(VERSION_1), VERSION_2).events();
        assertThat(events).containsExactly(EVENT_2);
        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_2);
    }
//
//    @Test
//    public void oldEventsAreDeletedAndPassedToSnapshot() {
//        eventLog.processUpdate(SNAPSHOT);
//        eventLog.processUpdate(SUCCESS);
//        List<LockWatchEvent> events = eventLog.getEventsBetweenVersions(
//                Optional.of(IdentifiedVersion.of(VERSION_1.id(), VERSION_1.version() - 1)),
//                VERSION_2).events();
//        assertThat(events).containsExactly(EVENT_1, EVENT_2);
//        assertThat(eventLog.getLatestKnownVersion()).hasValue(VERSION_2);
//
//        when(snapshot.getSnapshot()).thenReturn(LockWatchStateUpdate.snapshot(
//                VERSION_2.id(),
//                VERSION_2.version(),
//                ImmutableSet.of(),
//                ImmutableSet.of()));
//        eventLog.removeEventsBefore(VERSION_2.version());
//        eventLog.getEventsBetweenVersions(Optional.of(VERSION_1), VERSION_2);
//        verify(snapshot).getSnapshot();
//        verify(snapshot).processEvents(events, versionId);
//    }
}
