// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.cleaner;

import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.Maps;

/**
 * A simple PuncherStore that does not actually persist. This is useful for unit testing, because
 * the implementation is so simple that it effectively serves as the spec for how a PuncherStore
 * should behave.
 *
 * @author jweel
 */
class InMemoryPuncherStore implements PuncherStore {
    // TODO (ejin): Use a KeyValueServicePuncherStore backed by an InMemoryKeyValueService
    // for unit tests instead
    public static InMemoryPuncherStore create() {
        return new InMemoryPuncherStore();
    }

    private InMemoryPuncherStore() {//
    }

    private final SortedMap<Long, Long> map = makeMap();

    private final TreeMap<Long, Long> makeMap() {
        TreeMap<Long, Long> map1 = Maps.newTreeMap();
        map1.put(Long.MIN_VALUE, Long.MIN_VALUE);
        return map1;
    }

    @Override
    public void put(long timestamp, long timeMillis) {
        map.put(timeMillis, timestamp);
    }

    @Override
    public Long get(Long timeMillis) {
        // Note: To stay consistent with the interface comments as well as the
        // KVS implementation, we want to include timeMillis itself
        return map.get(map.headMap(timeMillis + 1).lastKey());
    }
}
