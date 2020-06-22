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
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

final class VersionedEventStore {
    private static final boolean INCLUSIVE = true;

    private final NavigableMap<Long, LockWatchEvent> eventMap = new TreeMap<>();

    Collection<LockWatchEvent> getEventsBetweenVersionsInclusive(Optional<Long> maybeStartVersion, long endVersion) {
        return ImmutableSet.copyOf(maybeStartVersion.map(
                startVersion -> getValuesBetweenInclusive(endVersion, startVersion))
                .orElseGet(() -> getFirstKey().map(firstKey -> getValuesBetweenInclusive(endVersion, firstKey))
                        .orElseGet(ImmutableList::of)));
    }

    LockWatchEvents getAndRemoveElementsUpToExclusive(long endVersion) {
        Set<Map.Entry<Long, LockWatchEvent>> elementsUpToVersion = eventMap.headMap(endVersion).entrySet();
        LockWatchEvents events = LockWatchEvents.create(elementsUpToVersion);
        elementsUpToVersion.clear();
        return events;
    }

    boolean contains(long key) {
        return eventMap.floorKey(key) != null;
    }

    long putAll(Iterable<LockWatchEvent> events) {
        events.forEach(event -> eventMap.put(event.sequence(), event));
        return getLastKey();
    }

    void clear() {
        eventMap.clear();
    }

    @VisibleForTesting
    VersionedEventStoreState getStateForTesting() {
        return ImmutableVersionedEventStoreState.builder()
                .eventMap(eventMap)
                .build();
    }

    private Collection<LockWatchEvent> getValuesBetweenInclusive(long endVersion, Long startVersion) {
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
