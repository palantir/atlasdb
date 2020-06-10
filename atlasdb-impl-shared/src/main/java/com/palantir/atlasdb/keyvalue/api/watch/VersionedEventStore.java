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

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.logsafe.Preconditions;

final class VersionedEventStore {
    private static final boolean INCLUSIVE = true;

    private final NavigableMap<Long, LockWatchEvent> eventMap = new TreeMap<>();

    boolean isEmpty() {
        return eventMap.isEmpty();
    }

    long getFirstKey() {
        Preconditions.checkState(!eventMap.isEmpty(), "Cannot get first key from empty map");
        return eventMap.firstKey();
    }

    long getLastKey() {
        Preconditions.checkState(!eventMap.isEmpty(), "Cannot get last key from empty map");
        return eventMap.lastKey();
    }

    Collection<LockWatchEvent> getEventsBetweenVersionsInclusive(long startVersion, long endVersion) {
        return eventMap.subMap(startVersion, INCLUSIVE, endVersion, INCLUSIVE).values();
    }

    Set<Map.Entry<Long, LockWatchEvent>> getElementsUpToExclusive(long endVersion) {
        return ImmutableSet.copyOf(eventMap.headMap(endVersion).entrySet());
    }

    void clearElementsUpToExclusive(long endVersion) {
        eventMap.headMap(endVersion).entrySet().clear();
    }

    boolean hasFloorKey(long key) {
        return eventMap.floorKey(key) != null;
    }

    void clear() {
        eventMap.clear();
    }

    void put(long version, LockWatchEvent event) {
        eventMap.put(version, event);
    }
}
