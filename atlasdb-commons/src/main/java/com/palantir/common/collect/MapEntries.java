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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import java.util.Map;
import java.util.Map.Entry;


public class MapEntries {
    private MapEntries() { /* */ }

    public static <L, R> Function<Entry<L, R>, L> getKeyFunction() {
        return from -> from.getKey();
    }

    public static <L, R> Function<Entry<L, R>, R> getValueFunction() {
        return from -> from.getValue();
    }

    public static <K, V> Map<K, V> putAll(Map<K, V> map, Iterable<? extends Entry<? extends K, ? extends V>> it) {
        for (Entry<? extends K, ? extends V> e : it) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }

    public static <K, V> void put(Map<? super K, ? super V> map, Entry<K, V> e) {
        map.put(e.getKey(), e.getValue());
    }

    public static <K, V> ImmutableMap<K, V> toMap(Iterable<Entry<K, V>> it) {
        Builder<K, V> builder = ImmutableMap.builder();
        for (Entry<K, V> e : it) {
            builder.put(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    public static <L, F, T>  Function<Entry<L, F>, Entry<L, T>> applyValue(final Function<F, T> f) {
        return from -> Maps.immutableEntry(from.getKey(), f.apply(from.getValue()));
    }

    public static <F, T, R>  Function<Entry<F, R>, Entry<T, R>> applyKey(final Function<F, T> f) {
        return from -> Maps.immutableEntry(f.apply(from.getKey()), from.getValue());
    }

    public static <K, V>  Predicate<Map.Entry<K, V>> applyValue(final Predicate<V> f) {
        return input -> f.apply(input.getValue());
    }

    public static <K, V>  Predicate<Map.Entry<K, V>> applyKey(final Predicate<K> f) {
        return input -> f.apply(input.getKey());
    }

    public static <K, V1, V2> Function<Entry<K, V1>, V2> funFromEntryTransformer(final EntryTransformer<K, V1, V2> et) {
        return input -> et.transformEntry(input.getKey(), input.getValue());
    }

    public static <K, V1, V2> EntryTransformer<K, V1, V2> entryTransformerFromFun(final Function<Entry<K, V1>, V2> f) {
        return (k, v1) -> f.apply(Maps.immutableEntry(k, v1));
    }
}
