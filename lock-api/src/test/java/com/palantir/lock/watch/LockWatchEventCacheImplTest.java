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
import static org.junit.Assert.fail;
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
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.exceptions.SafeNullPointerException;

@RunWith(MockitoJUnitRunner.class)
public final class LockWatchEventCacheImplTest {
    private static final String TABLE = "table";
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, new byte[] {1});
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, new byte[] {2});
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, new byte[] {3});
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockWatchReferences.LockWatchReference REFERENCE = LockWatchReferences.entireTable("table");
    private static final LockWatchEvent WATCH_EVENT =
            LockWatchCreatedEvent.builder(ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR))
                    .build(0L);
    private static final LockWatchEvent LOCK_EVENT = LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2),
            LockToken.of(UUID.randomUUID())).build(2L);
    private static final LockWatchEvent COMMIT_LOCK_EVENT =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3), LOCK_TOKEN).build(2L);
    private static final UUID LEADER = UUID.randomUUID();
    private static final IdentifiedVersion VERSION_1 = IdentifiedVersion.of(LEADER, 17L);
    private static final IdentifiedVersion VERSION_2 = IdentifiedVersion.of(LEADER, 26L);
    private static final IdentifiedVersion VERSION_3 = IdentifiedVersion.of(LEADER, 38L);
    private static final LockWatchStateUpdate.Success SUCCESS_1 =
            LockWatchStateUpdate.success(VERSION_1.id(), VERSION_1.version(), ImmutableList.of(WATCH_EVENT));
    private static final LockWatchStateUpdate.Success SUCCESS_2 = LockWatchStateUpdate.success(
            VERSION_2.id(),
            VERSION_2.version(),
            ImmutableList.of(LOCK_EVENT, COMMIT_LOCK_EVENT));
    private static final LockWatchStateUpdate.Snapshot SNAPSHOT =
            LockWatchStateUpdate.snapshot(VERSION_3.id(), VERSION_3.version(), ImmutableSet.of(), ImmutableSet.of());
    private static final Set<Long> TIMESTAMPS_1 = ImmutableSet.of(1L, 2L, 1337L);
    private static final Set<Long> TIMESTAMPS_2 = ImmutableSet.of(3L, 10110101L);
    private static final Set<Long> TIMESTAMPS_COMBINED = ImmutableSet.of(1L, 2L, 3L, 1337L, 10110101L);

    @Mock
    private ClientLockWatchEventLog eventLog;

    private LockWatchEventCacheImpl eventCache;

    @Before
    public void before() {
        eventCache = LockWatchEventCacheImpl.create(eventLog);
    }

    @Test
    public void processStartTransactionUpdateAddsToCache() {
        Map<Long, IdentifiedVersion> expectedMap = constructExpectedMap(TIMESTAMPS_1, VERSION_1);

        processFirstTimestampBatch(expectedMap);

        expectedMap.putAll(constructExpectedMap(TIMESTAMPS_2, VERSION_2));

        setupSecondSuccess();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS_2);
        verify(eventLog).processUpdate(SUCCESS_2);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_COMBINED)).containsExactlyEntriesOf(expectedMap);
    }

    @Test
    public void processCommitTransactionUpdatesValidEntries() {
        Map<Long, IdentifiedVersion> expectedMap = constructExpectedMap(TIMESTAMPS_1, VERSION_1);
        processFirstTimestampBatch(expectedMap);

        Set<TransactionUpdate> update = ImmutableSet.of(ImmutableTransactionUpdate.builder()
                .startTs(1L)
                .commitTs(9999L)
                .writesToken(LOCK_TOKEN)
                .build());

        setupSecondSuccess();
        eventCache.processGetCommitTimestampsUpdate(update, SUCCESS_2);
        verify(eventLog).processUpdate(SUCCESS_2);

        when(eventLog.getEventsBetweenVersions(Optional.of(VERSION_1), VERSION_2)).thenReturn(
                ImmutableTransactionsLockWatchEvents.builder()
                        .addEvents(WATCH_EVENT, LOCK_EVENT, COMMIT_LOCK_EVENT)
                        .clearCache(false));
        CommitUpdate commitUpdate = eventCache.getCommitUpdate(1L);
        verify(eventLog).getEventsBetweenVersions(Optional.of(VERSION_1), VERSION_2);
        assertThat(commitUpdate.accept(SuccessVisitor.INSTANCE).invalidatedLocks())
                .containsExactlyInAnyOrder(DESCRIPTOR, DESCRIPTOR_2);
    }

    @Test
    public void previousTimestampsClearedOnSnapshotUpdate() {
        Map<Long, IdentifiedVersion> expectedMap = constructExpectedMap(TIMESTAMPS_COMBINED, VERSION_1);

        setupFirstSuccess();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_COMBINED, SUCCESS_1);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_COMBINED)).containsExactlyEntriesOf(expectedMap);

        when(eventLog.processUpdate(SNAPSHOT)).thenReturn(Optional.of(VERSION_3));
        Set<Long> secondBatch = ImmutableSet.of(666L, 12545L);
        eventCache.processStartTransactionsUpdate(secondBatch, SNAPSHOT);

        Map<Long, IdentifiedVersion> newExpectedMap = constructExpectedMap(secondBatch, VERSION_3);

        assertThat(eventCache.getTimestampToVersionMap(secondBatch)).containsExactlyEntriesOf(newExpectedMap);
        assertThatThrownBy(() -> eventCache.getTimestampToVersionMap(TIMESTAMPS_COMBINED))
                .isInstanceOf(SafeNullPointerException.class)
                .hasMessage("Timestamp missing from cache");
    }

    @Test
    public void removeFromCacheUpdatesEarliestVersion() {
        Map<Long, IdentifiedVersion> expectedMap1 = constructExpectedMap(TIMESTAMPS_1, VERSION_1);

        setupFirstSuccess();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_1, SUCCESS_1);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_1)).containsExactlyInAnyOrderEntriesOf(expectedMap1);

        Map<Long, IdentifiedVersion> expectedMap2 = constructExpectedMap(TIMESTAMPS_2, VERSION_2);
        Map<Long, IdentifiedVersion> combinedMap = new HashMap<>(expectedMap1);
        combinedMap.putAll(expectedMap2);

        setupSecondSuccess();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS_2);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_2)).containsExactlyInAnyOrderEntriesOf(expectedMap2);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_COMBINED)).containsExactlyInAnyOrderEntriesOf(
                combinedMap);
        assertThat(eventCache.getEarliestVersion()).hasValue(VERSION_1);

        removeTimestampAndCheckEarliestVersion(2L, VERSION_1);
        removeTimestampAndCheckEarliestVersion(3L, VERSION_1);
        removeTimestampAndCheckEarliestVersion(1L, VERSION_1);
        removeTimestampAndCheckEarliestVersion(1337L, VERSION_2);
    }

    private void setupFirstSuccess() {
        setupEventLogSuccess(Optional.empty(), VERSION_1, SUCCESS_1);
    }

    private void setupSecondSuccess() {
        setupEventLogSuccess(Optional.of(VERSION_1), VERSION_2, SUCCESS_2);
    }

    private void setupEventLogSuccess(
            Optional<IdentifiedVersion> previousVersion,
            IdentifiedVersion newVersion, LockWatchStateUpdate.Success successUpdate) {
        when(eventLog.getLatestKnownVersion()).thenReturn(previousVersion);
        when(eventLog.processUpdate(successUpdate)).thenReturn(Optional.of(newVersion));
    }

    private void processFirstTimestampBatch(Map<Long, IdentifiedVersion> expectedMap) {
        setupFirstSuccess();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_1, SUCCESS_1);
        verify(eventLog).processUpdate(SUCCESS_1);
        assertThat(eventCache.getTimestampToVersionMap(TIMESTAMPS_1)).containsExactlyEntriesOf(expectedMap);
    }

    private static Map<Long, IdentifiedVersion> constructExpectedMap(Set<Long> timestamps, IdentifiedVersion version) {
        Map<Long, IdentifiedVersion> expectedMap = new HashMap<>();
        timestamps.forEach(timestamp -> expectedMap.put(timestamp, version));
        return expectedMap;
    }

    private void removeTimestampAndCheckEarliestVersion(long timestamp, IdentifiedVersion version) {
        eventCache.removeTimestampFromCache(timestamp);
        assertThat(eventCache.getEarliestVersion()).hasValue(version);
    }

    enum SuccessVisitor implements CommitUpdate.Visitor<CommitUpdate.InvalidateSome> {
        INSTANCE;

        @Override
        public CommitUpdate.InvalidateSome visit(CommitUpdate.InvalidateAll update) {
            fail("Expecting invalidateSome response");
            return null;
        }

        @Override
        public CommitUpdate.InvalidateSome visit(CommitUpdate.InvalidateSome update) {
            return update;
        }
    }
}
