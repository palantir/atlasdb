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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class SnapshotStoreImpl implements SnapshotStore {
    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotStoreImpl.class);
    private static final Sequence MAX_VERSION = Sequence.of(Long.MAX_VALUE);

    private final NavigableMap<Sequence, ValueCacheSnapshot> snapshotMap;
    private final Multimap<Sequence, StartTimestamp> liveSequences;
    private final Map<StartTimestamp, Sequence> timestampMap;
    private final int minimumSize;
    private final int maximumSize;

    @VisibleForTesting
    SnapshotStoreImpl(int minimumSize, int maximumSize, CacheMetrics cacheMetrics) {
        this.snapshotMap = new TreeMap<>();
        this.timestampMap = new HashMap<>();
        this.liveSequences = MultimapBuilder.treeKeys().hashSetValues().build();
        this.minimumSize = minimumSize;
        this.maximumSize = maximumSize;
        cacheMetrics.setSnapshotsHeldInMemory(snapshotMap::size);
        cacheMetrics.setSequenceDifference(() -> {
            if (snapshotMap.isEmpty()) {
                return 0L;
            } else {
                return snapshotMap.lastKey().value() - snapshotMap.firstKey().value();
            }
        });
    }

    static SnapshotStore create(CacheMetrics cacheMetrics) {
        return new SnapshotStoreImpl(1_000, 20_000, cacheMetrics);
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
        });

        retentionSnapshots();
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

    private void retentionSnapshots() {
        int snapshotMapSize = snapshotMap.size();
        if (snapshotMapSize > minimumSize) {
            int numToRetention = snapshotMapSize - minimumSize;
            Sequence firstLiveSequence = Optional.ofNullable(Iterables.getFirst(liveSequences.keySet(), null))
                    .orElse(MAX_VERSION);

            List<Sequence> sequencesToRemove = snapshotMap.keySet().stream()
                    .limit(numToRetention)
                    .filter(sequence -> sequence.value() < firstLiveSequence.value())
                    .collect(Collectors.toList());
            sequencesToRemove.forEach(snapshotMap::remove);
        }
    }

    private void validateStateSize() {
        if (snapshotMap.size() > maximumSize
                || liveSequences.size() > maximumSize
                || timestampMap.size() > maximumSize) {
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
                    SafeArg.of("maximumSize", maximumSize));
            throw new SafeIllegalStateException("Exceeded max snapshot store size");
        }
    }
}
