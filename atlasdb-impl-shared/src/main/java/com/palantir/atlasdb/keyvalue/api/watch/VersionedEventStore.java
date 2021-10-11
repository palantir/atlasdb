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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.cache.CacheMetrics;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

final class VersionedEventStore {
    private static final boolean INCLUSIVE = true;
    private static final Sequence MAX_VERSION = Sequence.of(Long.MAX_VALUE);

    private final int minEvents;
    private final int maxEvents;
    private final NavigableMap<Sequence, LockWatchEvent> eventMap = new TreeMap<>();

    VersionedEventStore(CacheMetrics cacheMetrics, int minEvents, int maxEvents) {
        Preconditions.checkArgument(minEvents > 0, "minEvents must be positive", SafeArg.of("minEvents", minEvents));
        Preconditions.checkArgument(
                maxEvents >= minEvents,
                "maxEvents must be greater than or equal to minEvents",
                SafeArg.of("minEvents", minEvents),
                SafeArg.of("maxEvents", maxEvents));
        this.maxEvents = maxEvents;
        this.minEvents = minEvents;
        cacheMetrics.setEventsHeldInMemory(eventMap::size);
    }

    Collection<LockWatchEvent> getEventsBetweenVersionsInclusive(Optional<Long> maybeStartVersion, long endVersion) {
        Optional<Long> startVersion = maybeStartVersion.or(this::getFirstKey).filter(version -> version <= endVersion);

        return startVersion
                .map(version -> getValuesBetweenInclusive(endVersion, version))
                .orElseGet(ImmutableList::of);
    }

    LockWatchEvents retentionEvents(Optional<Sequence> earliestSequenceToKeep) {
        if (eventMap.size() < minEvents) {
            return LockWatchEvents.builder().build();
        }

        // Guarantees that we remove some events while still also potentially performing further retention - note
        // that each call to retentionEventsInternal modifies eventMap.
        if (eventMap.size() > maxEvents) {
            List<LockWatchEvent> overMaxSizeEvents = retentionEventsInternal(eventMap.size() - maxEvents, MAX_VERSION);
            List<LockWatchEvent> restOfEvents =
                    retentionEventsInternal(eventMap.size() - minEvents, earliestSequenceToKeep.orElse(MAX_VERSION));
            return ImmutableLockWatchEvents.builder()
                    .addAllEvents(overMaxSizeEvents)
                    .addAllEvents(restOfEvents)
                    .build();
        } else {
            return ImmutableLockWatchEvents.builder()
                    .addAllEvents(retentionEventsInternal(
                            eventMap.size() - minEvents, earliestSequenceToKeep.orElse(MAX_VERSION)))
                    .build();
        }
    }

    private List<LockWatchEvent> retentionEventsInternal(int numToRetention, Sequence maxVersion) {
        List<LockWatchEvent> events = new ArrayList<>(numToRetention);

        // The correctness of this depends upon eventMap's entrySet returning entries in ascending sorted order.
        List<Map.Entry<Sequence, LockWatchEvent>> eventsToClear = eventMap.entrySet().stream()
                .limit(numToRetention)
                .filter(entry -> entry.getKey().value() < maxVersion.value())
                .collect(Collectors.toList());

        eventsToClear.forEach(entry -> {
            eventMap.remove(entry.getKey());
            events.add(entry.getValue());
        });

        return events;
    }

    boolean containsEntryLessThanOrEqualTo(long version) {
        return eventMap.floorKey(Sequence.of(version)) != null;
    }

    long putAll(LockWatchEvents events) {
        Preconditions.checkState(
                !events.events().isEmpty(),
                "Not expecting addition of empty lock watch events",
                UnsafeArg.of("lockWatchEvents", events));
        events.events().forEach(event -> eventMap.put(Sequence.of(event.sequence()), event));
        return getLastKey();
    }

    void clear() {
        eventMap.clear();
    }

    @VisibleForTesting
    VersionedEventStoreState getStateForTesting() {
        return ImmutableVersionedEventStoreState.builder().eventMap(eventMap).build();
    }

    private Collection<LockWatchEvent> getValuesBetweenInclusive(long endVersion, long startVersion) {
        return eventMap.subMap(Sequence.of(startVersion), INCLUSIVE, Sequence.of(endVersion), INCLUSIVE)
                .values();
    }

    private Optional<Long> getFirstKey() {
        return Optional.ofNullable(eventMap.firstEntry()).map(Entry::getKey).map(Sequence::value);
    }

    private long getLastKey() {
        Preconditions.checkState(!eventMap.isEmpty(), "Cannot get last key from empty map");
        return eventMap.lastKey().value();
    }
}
