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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.keyvalue.api.watch.Sequence;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class SnapshotStoreImpl implements SnapshotStore {
    private static final SafeLogger log = SafeLoggerFactory.get(SnapshotStoreImpl.class);

    private final ConcurrentNavigableMap<Sequence, ValueCacheSnapshot> snapshotMap;
    private final SetMultimap<Sequence, StartTimestamp> liveSequences;
    private final ConcurrentMap<StartTimestamp, Sequence> timestampMap;
    private final RateLimiter retentionRateLimiter = RateLimiter.create(1.0);
    private final int minimumUnusedSnapshots;
    private final int maximumSize;

    @VisibleForTesting
    SnapshotStoreImpl(int minimumUnusedSnapshots, int maximumSize, CacheMetrics cacheMetrics) {
        this.snapshotMap = new ConcurrentSkipListMap<>();
        this.timestampMap = new ConcurrentHashMap<>();
        this.liveSequences = Multimaps.synchronizedSetMultimap(
                MultimapBuilder.treeKeys().hashSetValues().build());
        this.minimumUnusedSnapshots = minimumUnusedSnapshots;
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
            liveSequences.putAll(sequence, timestamps);
            timestamps.forEach(timestamp -> timestampMap.put(timestamp, sequence));
        }
    }

    @Override
    public Optional<ValueCacheSnapshot> getSnapshot(StartTimestamp timestamp) {
        return Optional.ofNullable(timestampMap.get(timestamp)).flatMap(this::getSnapshotForSequence);
    }

    @Override
    public void removeTimestamp(StartTimestamp timestamp) {
        Optional.ofNullable(timestampMap.remove(timestamp))
                .ifPresent(sequence -> liveSequences.remove(sequence, timestamp));

        if (retentionRateLimiter.tryAcquire()) {
            retentionSnapshots();
        }
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

    @VisibleForTesting
    void retentionSnapshots() {
        Set<Sequence> currentLiveSequences;
        synchronized (liveSequences) {
            currentLiveSequences = ImmutableSet.copyOf(liveSequences.keySet());
        }
        int nonEssentialSnapshotCount = snapshotMap.size() - currentLiveSequences.size();
        int numToRetention = nonEssentialSnapshotCount - minimumUnusedSnapshots;
        if (numToRetention > 0) {
            Iterator<Sequence> sequences = snapshotMap.keySet().iterator();
            while (numToRetention > 0 && sequences.hasNext()) {
                Sequence sequence = sequences.next();
                if (!currentLiveSequences.contains(sequence)) {
                    sequences.remove();
                    numToRetention--;
                }
            }
        }
    }

    private void validateStateSize() {
        if (snapshotMap.size() > maximumSize) {
            // Since this is only occasionally done, it is plausible that with a very high throughput, we reach the
            // maximum when actually we can retention away some old snapshots
            retentionSnapshots();
        }

        if (maxSizeExceeded()) {
            RuntimeException maxSizeExceededException = new SafeIllegalStateException(
                    "Exceeded max snapshot store size",
                    SafeArg.of("snapshotMapSize", snapshotMap.size()),
                    SafeArg.of("liveSequencesSize", liveSequences.size()),
                    SafeArg.of("timestampMapSize", timestampMap.size()),
                    SafeArg.of("earliestTimestamp", first(timestampMap.keySet())),
                    SafeArg.of("earliestSnapshotSequence", first(snapshotMap.keySet())),
                    SafeArg.of("earliestLiveSequence", first(liveSequences.keySet())),
                    SafeArg.of("maximumSize", maximumSize));
            log.warn(
                    "Snapshot store has exceeded its maximum size. This likely indicates a memory leak.",
                    maxSizeExceededException);
            throw maxSizeExceededException;
        }
    }

    @Nullable
    private static <T extends Comparable<T>> T first(Set<T> set) {
        if (set.isEmpty()) {
            return null;
        }
        if (set instanceof SortedSet) {
            return ((SortedSet<T>) set).first();
        }
        synchronized (set) {
            return set.stream().min(Comparator.naturalOrder()).orElse(null);
        }
    }

    private boolean maxSizeExceeded() {
        return snapshotMap.size() > maximumSize
                || liveSequences.size() > maximumSize
                || timestampMap.size() > maximumSize;
    }
}
