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

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class SnapshotStoreImpl implements SnapshotStore {
    private final Map<Sequence, ValueCacheSnapshot> snapshotMap;
    private final SetMultimap<Sequence, StartTimestamp> liveSequences;
    private final Map<StartTimestamp, Sequence> timestampMap;

    SnapshotStoreImpl() {
        snapshotMap = new HashMap<>();
        timestampMap = new HashMap<>();
        liveSequences = MultimapBuilder.hashKeys().hashSetValues().build();
    }

    @Override
    public void storeSnapshot(Sequence sequence, Collection<StartTimestamp> timestamps, ValueCacheSnapshot snapshot) {
        if (!timestamps.isEmpty()) {
            snapshotMap.compute(sequence, (_seq, currentSnapshot) -> {
                Preconditions.checkState(
                        currentSnapshot == null || snapshot.equals(currentSnapshot),
                        "Attempted to store a snapshot where one already exists, and does not match");
                return snapshot;
            });
            timestamps.forEach(timestamp -> {
                liveSequences.put(sequence, timestamp);
                timestampMap.put(timestamp, sequence);
            });
        }
    }

    /**
     * If there are *very* infrequent updates, the cache may not progress the sequence at all. In this case, we want
     * to update the latest snapshot so that subsequent transactions may benefit from the reads. However, if the
     * transaction was the last for that sequence, the snapshot may have been removed, in which case we do not need
     * to re-write the snapshot, as that will happen when the next transaction is started.
     */
    @Override
    public void updateSnapshot(Sequence sequence, ValueCacheSnapshot snapshot) {
        snapshotMap.computeIfPresent(sequence, (_sequence, _snapshot) -> snapshot);
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshot(StartTimestamp timestamp) {
        return Optional.ofNullable(timestampMap.get(timestamp)).flatMap(this::getSnapshotForSequence);
    }

    @Override
    public Optional<Sequence> removeTimestamp(StartTimestamp timestamp) {
        Optional<Sequence> removedSequence = Optional.ofNullable(timestampMap.remove(timestamp));

        removedSequence.ifPresent(sequence -> {
            liveSequences.remove(sequence, timestamp);
            if (!liveSequences.containsKey(sequence)) {
                snapshotMap.remove(sequence);
            }
        });

        return removedSequence;
    }

    @Override
    public void reset() {
        snapshotMap.clear();
        liveSequences.clear();
        timestampMap.clear();
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshotForSequence(Sequence sequence) {
        return Optional.ofNullable(snapshotMap.get(sequence));
    }
}
