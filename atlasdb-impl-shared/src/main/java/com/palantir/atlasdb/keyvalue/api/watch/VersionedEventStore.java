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
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

final class VersionedEventStore {
    private static final boolean INCLUSIVE = true;

    private final int maxEvents;
    private final NavigableMap<Long, LockWatchEvent> eventMap = new TreeMap<>();

    VersionedEventStore(int maxEvents) {
        Preconditions.checkArgument(maxEvents > 0, "maxEvents must be positive");
        this.maxEvents = maxEvents;
    }

    Collection<LockWatchEvent> getEventsBetweenVersionsInclusive(Optional<Long> maybeStartVersion, long endVersion) {
        Optional<Long> startVersion = maybeStartVersion
                .map(Optional::of)
                .orElseGet(this::getFirstKey)
                .filter(version -> version <= endVersion);

        return startVersion
                .map(version -> getValuesBetweenInclusive(endVersion, version))
                .orElseGet(ImmutableList::of);
    }

    LockWatchEvents retentionEvents(Optional<Long> earliestSequenceToKeep) {
        if (eventMap.size() < maxEvents) {
            return LockWatchEvents.builder().build();
        }

        int numToRetention = eventMap.size() - maxEvents;
        long earliestVersion = earliestSequenceToKeep.orElse(Long.MAX_VALUE);

        ImmutableLockWatchEvents.Builder builder = LockWatchEvents.builder();
        List<Map.Entry<Long, LockWatchEvent>> eventsToClear = eventMap.entrySet().stream()
                .limit(numToRetention)
                .filter(entry -> entry.getKey() < earliestVersion)
                .collect(Collectors.toList());

        eventsToClear.forEach(entry -> {
            eventMap.remove(entry.getKey(), entry.getValue());
            builder.addEvents(entry.getValue());
        });

        return builder.build();
    }

    boolean containsEntryLessThanOrEqualTo(long key) {
        return eventMap.floorKey(key) != null;
    }

    long putAll(LockWatchEvents events) {
        events.events().forEach(event -> eventMap.put(event.sequence(), event));
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
        return eventMap.subMap(startVersion, INCLUSIVE, endVersion, INCLUSIVE).values();
    }

    private Optional<Long> getFirstKey() {
        if (eventMap.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(eventMap.firstKey());
        }
    }

    private long getLastKey() {
        Preconditions.checkState(!eventMap.isEmpty(), "Cannot get last key from empty map");
        return eventMap.lastKey();
    }
}
