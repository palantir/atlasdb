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
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import java.util.Map;

public final class MapEntries {
    private MapEntries() {
        /* */
    }

    public static <L, R> Function<Map.Entry<L, R>, L> getKeyFunction() {
        return Map.Entry::getKey;
    }

    public static <L, R> Function<Map.Entry<L, R>, R> getValueFunction() {
        return Map.Entry::getValue;
    }

    public static <K, V> Map<K, V> putAll(Map<K, V> map, Iterable<? extends Map.Entry<? extends K, ? extends V>> it) {
        for (Map.Entry<? extends K, ? extends V> e : it) {
            map.put(e.getKey(), e.getValue());
        }
        return map;
    }

    public static <K, V> void put(Map<? super K, ? super V> map, Map.Entry<K, V> e) {
        map.put(e.getKey(), e.getValue());
    }

    public static <K, V> ImmutableMap<K, V> toMap(Iterable<Map.Entry<K, V>> it) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (Map.Entry<K, V> e : it) {
            builder.put(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    public static <L, F, T> Function<Map.Entry<L, F>, Map.Entry<L, T>> applyValue(final Function<F, T> f) {
        return from -> Maps.immutableEntry(from.getKey(), f.apply(from.getValue()));
    }

    public static <F, T, R> Function<Map.Entry<F, R>, Map.Entry<T, R>> applyKey(final Function<F, T> f) {
        return from -> Maps.immutableEntry(f.apply(from.getKey()), from.getValue());
    }

    public static <K, V> Predicate<Map.Entry<K, V>> applyValue(final Predicate<V> f) {
        return input -> f.apply(input.getValue());
    }

    public static <K, V> Predicate<Map.Entry<K, V>> applyKey(final Predicate<K> f) {
        return input -> f.apply(input.getKey());
    }

    public static <K, V1, V2> Function<Map.Entry<K, V1>, V2> funFromEntryTransformer(
            final EntryTransformer<K, V1, V2> et) {
        return input -> et.transformEntry(input.getKey(), input.getValue());
    }

    public static <K, V1, V2> EntryTransformer<K, V1, V2> entryTransformerFromFun(
            final Function<Map.Entry<K, V1>, V2> f) {
        return (k, v1) -> f.apply(Maps.immutableEntry(k, v1));
    }
}
