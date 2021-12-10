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

package com.palantir.atlasdb.keyvalue.api.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.atlasdb.util.MetricsManagers;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.assertj.core.api.OptionalAssert;
import org.junit.Before;
import org.junit.Test;

public final class SnapshotStoreImplTest {
    private static final Sequence SEQUENCE_1 = Sequence.of(1337L);
    private static final Sequence SEQUENCE_2 = Sequence.of(8284L);
    private static final Sequence SEQUENCE_3 = Sequence.of(9999L);
    private static final Sequence SEQUENCE_4 = Sequence.of(31337L);
    private static final Sequence SEQUENCE_5 = Sequence.of(88888L);
    private static final StartTimestamp TIMESTAMP_1 = StartTimestamp.of(42L);
    private static final StartTimestamp TIMESTAMP_2 = StartTimestamp.of(31415925635L);
    private static final StartTimestamp TIMESTAMP_3 = StartTimestamp.of(404L);
    private static final StartTimestamp TIMESTAMP_4 = StartTimestamp.of(10110101L);
    private static final StartTimestamp TIMESTAMP_5 = StartTimestamp.of(500);
    private static final ValueCacheSnapshot SNAPSHOT_1 =
            ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty(), ImmutableSet.of());
    private static final ValueCacheSnapshot SNAPSHOT_2 = createSnapshot(2);
    private static final ValueCacheSnapshot SNAPSHOT_3 = createSnapshot(3);
    private static final ValueCacheSnapshot SNAPSHOT_4 = createSnapshot(4);
    private static final ValueCacheSnapshot SNAPSHOT_5 = createSnapshot(5);
    private static final CacheMetrics CACHE_METRICS = CacheMetrics.create(MetricsManagers.createForTests());

    private SnapshotStore snapshotStore;

    @Before
    public void before() {
        snapshotStore = SnapshotStoreImpl.create(CACHE_METRICS);
    }

    @Test
    public void singleSnapshotStoredForMultipleTimestamps() {
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3), SNAPSHOT_1);
        snapshotStore.storeSnapshot(SEQUENCE_2, ImmutableSet.of(TIMESTAMP_4), SNAPSHOT_2);

        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_4);
    }

    @Test
    public void snapshotsOverwriteForSameSequence() {
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_1);
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_2), SNAPSHOT_2);

        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1).get())
                .isEqualTo(SNAPSHOT_2)
                .isNotEqualTo(SNAPSHOT_1);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_1, TIMESTAMP_2);
    }

    @Test
    public void removeTimestampRemovesSnapshotWhenThereAreNoMoreLiveTimestampsForSequence() {
        snapshotStore = new SnapshotStoreImpl(0, 20_000, CACHE_METRICS);
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3), SNAPSHOT_1);
        snapshotStore.storeSnapshot(SEQUENCE_2, ImmutableSet.of(TIMESTAMP_4), SNAPSHOT_2);

        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3);

        snapshotStore.removeTimestamp(TIMESTAMP_2);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_1, TIMESTAMP_3);
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_2)).isEmpty();

        snapshotStore.removeTimestamp(TIMESTAMP_1);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_3);
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1)).isEmpty();

        snapshotStore.removeTimestamp(TIMESTAMP_3);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_4);
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1)).isEmpty();

        forceRunSnapshotRetention();
        assertThat(snapshotStore.getSnapshotForSequence(SEQUENCE_1)).isEmpty();
    }

    @Test
    public void removeTimestampOnlyRetentionsDownToMinimumSize() {
        snapshotStore = new SnapshotStoreImpl(1, 20_000, CACHE_METRICS);
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_1);
        snapshotStore.storeSnapshot(SEQUENCE_2, ImmutableSet.of(TIMESTAMP_2), SNAPSHOT_2);
        snapshotStore.storeSnapshot(SEQUENCE_3, ImmutableSet.of(TIMESTAMP_3), SNAPSHOT_3);
        snapshotStore.storeSnapshot(SEQUENCE_4, ImmutableSet.of(TIMESTAMP_4), SNAPSHOT_4);

        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_1);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_2);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_3, TIMESTAMP_3);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_4, TIMESTAMP_4);

        removeSnapshotAndAssert(TIMESTAMP_2, SEQUENCE_2)
                .as("snapshot is the latest non-essential and thus kept")
                .hasValue(SNAPSHOT_2);

        removeSnapshotAndAssert(TIMESTAMP_1, SEQUENCE_1)
                .as("snapshot is not the latest and thus removed")
                .isEmpty();

        removeSnapshotAndAssert(TIMESTAMP_4, SEQUENCE_4)
                .as("snapshot is the latest non-essential and thus kept")
                .hasValue(SNAPSHOT_4);

        removeSnapshotAndAssert(TIMESTAMP_3, SEQUENCE_3)
                .as("snapshot is not the latest and thus removed")
                .isEmpty();

        snapshotStore.storeSnapshot(SEQUENCE_5, ImmutableSet.of(TIMESTAMP_5), SNAPSHOT_5);
        removeSnapshotAndAssert(TIMESTAMP_5, SEQUENCE_5)
                .as("snapshot is the latest non-essential and thus kept")
                .hasValue(SNAPSHOT_5);

        assertThat(snapshotStore.getSnapshotForSequence(SEQUENCE_4))
                .as("snapshot is no longer latest and thus removed")
                .isEmpty();
    }

    @Test
    public void retentionIsNotRunOnEveryRemoval() {
        snapshotStore = new SnapshotStoreImpl(0, 20_000, CACHE_METRICS);
        long numEntries = 100L;
        List<Sequence> sequences =
                LongStream.range(0, numEntries).mapToObj(Sequence::of).collect(Collectors.toList());
        List<StartTimestamp> timestamps =
                LongStream.range(0, numEntries).mapToObj(StartTimestamp::of).collect(Collectors.toList());

        Streams.forEachPair(
                sequences.stream(),
                timestamps.stream(),
                (sequence, timestamp) -> snapshotStore.storeSnapshot(sequence, ImmutableSet.of(timestamp), SNAPSHOT_1));

        timestamps.forEach(timestamp -> assertSnapshotsEqualForTimestamp(SNAPSHOT_1, timestamp));
        timestamps.forEach(timestamp -> snapshotStore.removeTimestamp(timestamp));
        timestamps.forEach(
                timestamp -> assertThat(snapshotStore.getSnapshot(timestamp)).isEmpty());
        assertThat(anySnapshotPresentForSequences(sequences)).isTrue();
        forceRunSnapshotRetention();
        assertThat(anySnapshotPresentForSequences(sequences)).isFalse();
    }

    private boolean anySnapshotPresentForSequences(List<Sequence> sequences) {
        return sequences.stream()
                .anyMatch(sequence ->
                        snapshotStore.getSnapshotForSequence(sequence).isPresent());
    }

    private OptionalAssert<ValueCacheSnapshot> removeSnapshotAndAssert(StartTimestamp timestamp, Sequence sequence) {
        snapshotStore.removeTimestamp(timestamp);
        forceRunSnapshotRetention();
        assertThat(snapshotStore.getSnapshot(timestamp)).isEmpty();
        return assertThat(snapshotStore.getSnapshotForSequence(sequence));
    }

    private void forceRunSnapshotRetention() {
        // retention is now rate-limited, so we must bypass if we want to guarantee the removal of a snapshot
        ((SnapshotStoreImpl) snapshotStore).retentionSnapshots();
    }

    private void assertSnapshotsEqualForTimestamp(ValueCacheSnapshot expectedValue, StartTimestamp... timestamps) {
        Stream.of(timestamps).map(snapshotStore::getSnapshot).forEach(snapshot -> assertThat(snapshot)
                .hasValue(expectedValue));
    }

    private static ValueCacheSnapshot createSnapshot(int value) {
        byte byteValue = (byte) value;
        return ValueCacheSnapshotImpl.of(
                HashMap.<CellReference, CacheEntry>empty()
                        .put(
                                CellReference.of(
                                        TableReference.createFromFullyQualifiedName("t.table"),
                                        Cell.create(new byte[] {byteValue}, new byte[] {byteValue})),
                                CacheEntry.locked()),
                HashSet.empty(),
                ImmutableSet.of());
    }
}
