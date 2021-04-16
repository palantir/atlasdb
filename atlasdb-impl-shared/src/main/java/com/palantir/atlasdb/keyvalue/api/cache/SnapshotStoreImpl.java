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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class SnapshotStoreImpl implements SnapshotStore {
    private final Map<Sequence, ValueCacheSnapshot> snapshotMap;
    private final SetMultimap<Sequence, StartTimestamp> liveSequences;
    private final Map<StartTimestamp, Sequence> timestampMap;

    public SnapshotStoreImpl() {
        snapshotMap = new HashMap<>();
        timestampMap = new HashMap<>();
        liveSequences = MultimapBuilder.hashKeys().hashSetValues().build();
    }

    @Override
    public void storeSnapshot(Sequence sequence, StartTimestamp timestamp, ValueCacheSnapshot snapshot) {
        snapshotMap.putIfAbsent(sequence, snapshot);
        liveSequences.put(sequence, timestamp);
        timestampMap.put(timestamp, sequence);
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshot(StartTimestamp timestamp) {
        return Optional.ofNullable(timestampMap.get(timestamp)).map(snapshotMap::get);
    }

    @Override
    public void removeTimestamp(StartTimestamp timestamp) {
        Optional.ofNullable(timestampMap.remove(timestamp)).ifPresent(sequence -> {
            liveSequences.remove(sequence, timestamp);
            if (!liveSequences.containsKey(sequence)) {
                snapshotMap.remove(sequence);
            }
        });
    }

    @Override
    public void reset() {
        snapshotMap.clear();
        liveSequences.clear();
        timestampMap.clear();
    }
}
