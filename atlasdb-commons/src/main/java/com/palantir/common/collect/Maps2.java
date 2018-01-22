/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.common.collect;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Maps;

public class Maps2 {
    private Maps2() {/* */}

    public static <K, V> ImmutableMap<K, V> fromEntries(Iterable<? extends Map.Entry<K, V>> it) {
        Builder<K, V> builder = ImmutableMap.builder();
        for (Entry<K, V> e : it) {
            builder.put(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    public static <K1, K2, V> ImmutableMap<K2, V> transformKeys(Map<K1, V> map, Function<K1, K2> f) {
        return fromEntries(IterableView.of(map.entrySet()).transform(MapEntries.<K1, K2, V>applyKey(f)));
    }

    public static <K, V> Map<K, V> createConstantValueMap(Set<K> keys, V v) {
        Map<K, V> ret = Maps.newHashMapWithExpectedSize(keys.size());
        for (K k : keys) {
            ret.put(k, v);
        }
        return ret;
    }

    public static <K, V> Map<K, V> createConstantValueMap(Iterable<K> keys, V v) {
        Map<K, V> ret = Maps.newHashMap();
        for (K k : keys) {
            ret.put(k, v);
        }
        return ret;
    }

    public static <K1, V1, V2> Map<K1, V2> innerJoin(Map<K1, V1> first, Map<V1, V2> second) {
        return first.entrySet()
                .stream()
                .filter(entry -> second.containsKey(entry.getValue()))
                .collect(Collectors.toMap(
                        Entry::getKey,
                        entry -> second.get(entry.getValue())
                ));
    }
}
