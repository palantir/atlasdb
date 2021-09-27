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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class SnapshotStoreImpl implements SnapshotStore {
    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotStoreImpl.class);
    private static final int MAXIMUM_SIZE = 20_000;

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
        validateStateSize();

        if (!timestamps.isEmpty()) {
            snapshotMap.put(sequence, snapshot);
            timestamps.forEach(timestamp -> {
                liveSequences.put(sequence, timestamp);
                timestampMap.put(timestamp, sequence);
            });
        }
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshot(StartTimestamp timestamp) {
        return Optional.ofNullable(timestampMap.get(timestamp)).flatMap(this::getSnapshotForSequence);
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

    @Override
    public Optional<ValueCacheSnapshot> getSnapshotForSequence(Sequence sequence) {
        return Optional.ofNullable(snapshotMap.get(sequence));
    }

    private void validateStateSize() {
        if (snapshotMap.size() > MAXIMUM_SIZE
                || liveSequences.size() > MAXIMUM_SIZE
                || timestampMap.size() > MAXIMUM_SIZE) {
            log.warn(
                    "Snapshot store has exceeded its maximum size. This likely indicates a memory leak.",
                    SafeArg.of("snapshotMapSize", snapshotMap.size()),
                    SafeArg.of("liveSequencesSize", liveSequences.size()),
                    SafeArg.of("timestampMapSize", timestampMap.size()),
                    SafeArg.of(
                            "earliestTimestamp", timestampMap.keySet().stream().min(Comparator.naturalOrder())),
                    SafeArg.of(
                            "earliestSnapshotSequence",
                            snapshotMap.keySet().stream().min(Comparator.naturalOrder())),
                    SafeArg.of(
                            "earliestLiveSequence",
                            liveSequences.keySet().stream().min(Comparator.naturalOrder())),
                    SafeArg.of("maximumSize", MAXIMUM_SIZE));
            throw new SafeIllegalStateException("Exceeded max snapshot store size");
        }
    }
}
