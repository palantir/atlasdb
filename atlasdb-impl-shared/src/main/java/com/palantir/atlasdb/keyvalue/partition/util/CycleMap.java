/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.partition.util;

import java.util.NavigableMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ForwardingNavigableMap;

public final class CycleMap<K, V> extends ForwardingNavigableMap<K, V> {
    private final NavigableMap<K, V> backingMap;

    private CycleMap(NavigableMap<K, V> backingMap) {
        this.backingMap = backingMap;
    }

    @Override
    protected NavigableMap<K, V> delegate() {
        return backingMap;
    }

    public static <K, V> CycleMap<K, V> wrap(NavigableMap<K, V> backingMap) {
        return new CycleMap<>(backingMap);
    }

    public K nextKey(K key) {
        Preconditions.checkState(size() > 0);
        K next = higherKey(key);
        if (next == null) {
            next = firstKey();
        }
        return next;
    }

    public K previousKey(K key) {
        Preconditions.checkState(size() > 0);
        K previous = lowerKey(key);
        if (previous == null) {
            previous = lastKey();
        }
        return previous;
    }

    @Override
    public CycleMap<K, V> descendingMap() {
        return CycleMap.wrap(backingMap.descendingMap());
    }

}
