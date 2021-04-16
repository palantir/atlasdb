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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public final class SnapshotStoreImplTest {
    private static final Sequence SEQUENCE_1 = Sequence.of(1337L);
    private static final Sequence SEQUENCE_2 = Sequence.of(8284L);
    private static final StartTimestamp TIMESTAMP_1 = StartTimestamp.of(42L);
    private static final StartTimestamp TIMESTAMP_2 = StartTimestamp.of(31415925635L);
    private static final StartTimestamp TIMESTAMP_3 = StartTimestamp.of(404L);
    private static final StartTimestamp TIMESTAMP_4 = StartTimestamp.of(10110101L);
    private static final ValueCacheSnapshot SNAPSHOT_1 = ValueCacheSnapshotImpl.of(HashMap.empty(), HashSet.empty());
    private static final ValueCacheSnapshot SNAPSHOT_2 = ValueCacheSnapshotImpl.of(
            HashMap.<CellReference, CacheEntry>empty()
                    .put(
                            CellReference.of(
                                    TableReference.createFromFullyQualifiedName("t.table"),
                                    Cell.create(new byte[] {1}, new byte[] {1})),
                            CacheEntry.locked()),
            HashSet.empty());
    private SnapshotStore snapshotStore;

    @Before
    public void before() {
        snapshotStore = new SnapshotStoreImpl();
    }

    @Test
    public void singleSnapshotStoredForMultipleTimestamps() {
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3), SNAPSHOT_1);
        snapshotStore.storeSnapshot(SEQUENCE_2, ImmutableSet.of(TIMESTAMP_4), SNAPSHOT_2);

        assertSnapshotsEqualForTimestamp(SNAPSHOT_1, TIMESTAMP_1, TIMESTAMP_2, TIMESTAMP_3);
        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_4);
    }

    @Test
    public void snapshotsNotOverwrittenForSameSequence() {
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_1);

        assertThatThrownBy(() -> snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_2), SNAPSHOT_2))
                .isExactlyInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Attempted to store a snapshot where one already exists, and does not match");
    }

    @Test
    public void removeTimestampRemovesSnapshotWhenThereAreNoMoreLiveTimestampsForSequence() {
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
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1)).isEmpty();

        assertSnapshotsEqualForTimestamp(SNAPSHOT_2, TIMESTAMP_4);
        assertThat(snapshotStore.getSnapshotForSequence(SEQUENCE_1)).isEmpty();
    }

    @Test
    public void updateSnapshotOnlyUpdatesIfPresent() {
        snapshotStore.storeSnapshot(SEQUENCE_1, TIMESTAMP_1, SNAPSHOT_1);

        snapshotStore.updateSnapshot(SEQUENCE_1, SNAPSHOT_2);
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1)).hasValue(SNAPSHOT_2);

        snapshotStore.updateSnapshot(SEQUENCE_2, SNAPSHOT_1);
        assertThat(snapshotStore.getSnapshotForSequence(SEQUENCE_2)).isEmpty();
    }

    @Test
    public void updateSnapshotOnlyUpdatesIfPresent() {
        snapshotStore.storeSnapshot(SEQUENCE_1, ImmutableSet.of(TIMESTAMP_1), SNAPSHOT_1);

        snapshotStore.updateSnapshot(SEQUENCE_1, SNAPSHOT_2);
        assertThat(snapshotStore.getSnapshot(TIMESTAMP_1)).hasValue(SNAPSHOT_2);

        snapshotStore.updateSnapshot(SEQUENCE_2, SNAPSHOT_1);
        assertThat(snapshotStore.getSnapshotForSequence(SEQUENCE_2)).isEmpty();
    }

    private void assertSnapshotsEqualForTimestamp(ValueCacheSnapshot expectedValue, StartTimestamp... timestamps) {
        Stream.of(timestamps).map(snapshotStore::getSnapshot).forEach(snapshot -> assertThat(snapshot)
                .hasValue(expectedValue));
    }
}
