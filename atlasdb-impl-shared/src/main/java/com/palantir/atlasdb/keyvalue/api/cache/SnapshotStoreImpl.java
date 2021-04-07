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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

public final class SnapshotStoreImpl implements SnapshotStore {
    private final Map<Sequence, VersionedSnapshot> snapshotMap;
    private final Multimap<Sequence, StartTimestamp> liveSequences;
    private final Map<StartTimestamp, Sequence> timestampMap;

    public SnapshotStoreImpl() {
        snapshotMap = new HashMap<>();
        timestampMap = new HashMap<>();
        liveSequences = HashMultimap.create();
    }

    @Override
    public void reset() {
        snapshotMap.clear();
    }

    @Override
    public void storeSnapshot(Sequence sequence, StartTimestamp timestamp, ValueCacheSnapshot snapshot) {
        snapshotMap.putIfAbsent(sequence, VersionedSnapshot.of(sequence, snapshot));
        liveSequences.put(sequence, timestamp);
        timestampMap.put(timestamp, sequence);
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshot(Sequence sequence) {
        return Optional.ofNullable(snapshotMap.get(sequence)).map(VersionedSnapshot::snapshot);
    }

    @Override
    public void removeTimestamp(StartTimestamp timestamp) {
        Optional<Sequence> sequence = Optional.of(timestampMap.remove(timestamp));
        sequence.ifPresent(seq -> {
            liveSequences.remove(seq, timestamp);
            if (liveSequences.containsKey(seq)) {
                snapshotMap.remove(seq);
            }
        });
    }

    @Value.Immutable
    interface VersionedSnapshot {
        @Value.Parameter
        Sequence sequence();

        @Value.Parameter
        ValueCacheSnapshot snapshot();

        static VersionedSnapshot of(Sequence sequence, ValueCacheSnapshot snapshot) {
            return ImmutableVersionedSnapshot.of(sequence, snapshot);
        }
    }
}
