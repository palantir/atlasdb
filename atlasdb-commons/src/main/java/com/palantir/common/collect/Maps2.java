/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.common.collect;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class Maps2 {
    private Maps2() {
        /* */
    }

    public static <K, V> ImmutableMap<K, V> fromEntries(Iterable<? extends Map.Entry<K, V>> it) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Map.Entry<K, V> e : it) {
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
        Map<K, V> ret = new HashMap<>();
        for (K k : keys) {
            ret.put(k, v);
        }
        return ret;
    }
}
