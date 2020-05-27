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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.logsafe.exceptions.SafeNullPointerException;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventCacheImplTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final IdentifiedVersion VERSION_1 = IdentifiedVersion.of(LEADER, 17L);
    private static final IdentifiedVersion VERSION_2 = IdentifiedVersion.of(LEADER, 38L);
    private static final LockWatchStateUpdate.Success SUCCESS =
            LockWatchStateUpdate.success(VERSION_1.id(), VERSION_1.version(), ImmutableList.of());
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(VERSION_2.id(), VERSION_2.version(), ImmutableSet.of(), ImmutableSet.of());
    private static final Set<Long> TIMESTAMPS = ImmutableSet.of(1L, 2L, 3L, 1337L, 10110101L);

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
        verify(eventLog).processUpdate(SUCCESS, Optional.empty());
    }

    @Test
    public void processStartTransactionUpdateAddsToCache() {
        when(eventLog.processUpdate(SUCCESS, Optional.empty())).thenReturn(Optional.of(VERSION_1));
        Map<Long, IdentifiedVersion> expectedMap = constructExpectedMap();

        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        verify(eventLog).processUpdate(eq(SUCCESS), any());
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS)).containsExactlyEntriesOf(expectedMap);
    }

    @Test
    public void oldTimestampsClearedOnSnapshotUpdate() {
        Map<Long, IdentifiedVersion> expectedMap = constructExpectedMap();

        when(eventLog.processUpdate(SUCCESS, Optional.empty())).thenReturn(Optional.of(VERSION_1));
        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SUCCESS);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS)).containsExactlyEntriesOf(expectedMap);

        when(eventLog.processUpdate(SNAPSHOT, Optional.of(VERSION_1))).thenReturn(Optional.of(VERSION_2));
        Set<Long> secondBatch = ImmutableSet.of(666L, 12545L);
        eventCache.processStartTransactionsUpdate(secondBatch, SNAPSHOT);

        Map<Long, IdentifiedVersion> newExpectedMap = constructExpectedMap(secondBatch, VERSION_2);

        assertThat(eventCache.getTimestampToVersionMap(secondBatch)).containsExactlyEntriesOf(newExpectedMap);
        assertThatThrownBy(() -> eventCache.getTimestampToVersionMap(TIMESTAMPS))
                .isInstanceOf(SafeNullPointerException.class)
                .hasMessage("Timestamp missing from cache");
    }

    @Test
    public void removeFromCacheUpdatesEarliestVersion() {
        LockWatchStateUpdate.Success laterSuccess =
                LockWatchStateUpdate.success(VERSION_2.id(), VERSION_2.version(), ImmutableList.of());

        when(eventLog.processUpdate(SUCCESS, Optional.empty())).thenReturn(Optional.of(VERSION_1));
        when(eventLog.processUpdate(laterSuccess, Optional.of(VERSION_1))).thenReturn(Optional.of(VERSION_2));

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(1L, 2L), SUCCESS);

        when(eventLog.getLatestKnownVersion()).thenReturn(Optional.of(VERSION_1));
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(3L, 4L), laterSuccess);

        Map<Long, IdentifiedVersion> expectedMap = ImmutableMap.<Long, IdentifiedVersion>builder()
                .put(1L, VERSION_1)
                .put(2L, VERSION_1)
                .put(3L, VERSION_2)
                .put(4L, VERSION_2)
                .build();

        assertThat(eventCache.getTimestampToVersionMap(ImmutableSet.of(1L, 2L, 3L, 4L)))
                .containsExactlyInAnyOrderEntriesOf(expectedMap);
        assertThat(eventCache.getEarliestVersion()).hasValue(VERSION_1);

        removeTimestampAndCheckEarliestVersion(2L, VERSION_1);
        removeTimestampAndCheckEarliestVersion(4L, VERSION_1);
        removeTimestampAndCheckEarliestVersion(1L, VERSION_2);
    }

    private Map<Long, IdentifiedVersion> constructExpectedMap() {
        return constructExpectedMap(TIMESTAMPS, VERSION_1);
    }

    private Map<Long, IdentifiedVersion> constructExpectedMap(Set<Long> timestamps, IdentifiedVersion version) {
        Map<Long, IdentifiedVersion> expectedMap = new HashMap<>();
        timestamps.forEach(timestamp -> expectedMap.put(timestamp, version));
        return expectedMap;
    }

    private void removeTimestampAndCheckEarliestVersion(long timestamp, IdentifiedVersion version) {
        eventCache.removeTimestampFromCache(timestamp);
        assertThat(eventCache.getEarliestVersion()).hasValue(version);
    }

}
