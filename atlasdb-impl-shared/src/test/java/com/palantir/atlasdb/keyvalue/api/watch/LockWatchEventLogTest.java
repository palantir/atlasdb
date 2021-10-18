/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import java.util.UUID;
import org.junit.Test;

public class LockWatchEventLogTest {
    private static final int MIN_EVENTS = 5;
    private static final int MAX_EVENTS = 25;

    private static final UUID INITIAL_LOG_ID = UUID.randomUUID();
    private static final UUID DIFFERENT_LOG_ID = UUID.randomUUID();
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    private static final long SEQUENCE_1 = 1;
    private static final long SEQUENCE_2 = 2;

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("lwelt-one");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("lwelt-two");
    private static final LockWatchReference REFERENCE_1 = LockWatchReferences.entireTable("table.one");
    private static final LockWatchReference REFERENCE_2 = LockWatchReferences.entireTable("table.two");

    private final LockWatchEventLog eventLog =
            LockWatchEventLog.create(CacheMetrics.create(MetricsManagers.createForTests()), MIN_EVENTS, MAX_EVENTS);

    @Test
    public void doesNotHaveInitialVersion() {
        assertThat(eventLog.getLatestKnownVersion()).isEmpty();
    }

    @Test
    public void successUpdateWithoutContextLeadsToFailure() {
        CacheUpdate cacheUpdate =
                eventLog.processUpdate(LockWatchStateUpdate.success(INITIAL_LOG_ID, SEQUENCE_1, ImmutableList.of()));
        assertThat(cacheUpdate).isEqualTo(CacheUpdate.FAILED);
    }

    @Test
    public void snapshotUpdateSetsContextAndInstructsClientsToClearCache() {
        CacheUpdate cacheUpdate = eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                INITIAL_LOG_ID, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1)));

        LockWatchVersion initialLeaderAtSequenceOne = LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_1);
        assertThat(cacheUpdate.shouldClearCache()).isTrue();
        assertThat(cacheUpdate.getVersion()).hasValue(initialLeaderAtSequenceOne);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialLeaderAtSequenceOne)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(initialLeaderAtSequenceOne)
                                .addLocked(DESCRIPTOR_1)
                                .addWatches(REFERENCE_1)
                                .build())
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of())
                                .build())
                        .build());
    }

    @Test
    public void successUpdateUpdatesContextAndShouldInstructClientsNotToClearCache() {
        eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                INITIAL_LOG_ID, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1)));
        LockWatchEvent lockEvent =
                LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2), LOCK_TOKEN).build(SEQUENCE_2);
        CacheUpdate cacheUpdate = eventLog.processUpdate(
                LockWatchStateUpdate.success(INITIAL_LOG_ID, SEQUENCE_2, ImmutableList.of(lockEvent)));

        LockWatchVersion initialVersionAtSequenceTwo = LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_2);
        assertThat(cacheUpdate.shouldClearCache()).isFalse();
        assertThat(cacheUpdate.getVersion()).hasValue(initialVersionAtSequenceTwo);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(initialVersionAtSequenceTwo)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(LockWatchVersion.of(INITIAL_LOG_ID, SEQUENCE_1))
                                .addLocked(DESCRIPTOR_1)
                                .addWatches(REFERENCE_1)
                                .build())
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of(Sequence.of(SEQUENCE_2), lockEvent))
                                .build())
                        .build());
    }

    @Test
    public void snapshotUpdateResetsDifferingContextAndInstructsClientsToClearCache() {
        eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                INITIAL_LOG_ID, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_1), ImmutableSet.of(REFERENCE_1)));
        CacheUpdate secondSnapshotUpdateResult = eventLog.processUpdate(LockWatchStateUpdate.snapshot(
                DIFFERENT_LOG_ID, SEQUENCE_1, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of(REFERENCE_2)));

        LockWatchVersion differentLeaderAtSequenceOne = LockWatchVersion.of(DIFFERENT_LOG_ID, SEQUENCE_1);
        assertThat(secondSnapshotUpdateResult.shouldClearCache()).isTrue();
        assertThat(secondSnapshotUpdateResult.getVersion()).hasValue(differentLeaderAtSequenceOne);
        assertThat(eventLog.getStateForTesting())
                .isEqualTo(ImmutableLockWatchEventLogState.builder()
                        .latestVersion(differentLeaderAtSequenceOne)
                        .snapshotState(ImmutableClientLockWatchSnapshotState.builder()
                                .snapshotVersion(differentLeaderAtSequenceOne)
                                .addLocked(DESCRIPTOR_2)
                                .addWatches(REFERENCE_2)
                                .build())
                        .eventStoreState(ImmutableVersionedEventStoreState.builder()
                                .eventMap(ImmutableSortedMap.of())
                                .build())
                        .build());
    }

    @Test
    public void eventsCanBeAggregatedFromMultipleSnapshotUpdates() {

    }
}
