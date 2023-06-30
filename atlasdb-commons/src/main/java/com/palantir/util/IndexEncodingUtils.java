/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class IndexEncodingUtils {
    private IndexEncodingUtils() {}

    public static <K, V> Map<Integer, V> encode(List<K> orderedKeys, Map<K, V> values) {
        return encode(orderedKeys, values, Function.identity());
    }

    /**
     * Given an ordered list of keys and a map of keys to values, compute a map that uses indices into the list of keys
     * instead of actual keys.
     * Values of the returned map are created using the provided mapping function.
     *
     * @param orderedKeys a list of keys in any fixed (!) order. The same list MUST be used for decoding
     * @param values a map of keys to values. Every key MUST be contained in the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     */
    public static <K, V, R> Map<Integer, R> encode(List<K> orderedKeys, Map<K, V> values, Function<V, R> valueMapper) {
        // A linked hash map will give a minor improvement when iterating during serialization
        Map<Integer, R> res = Maps.newLinkedHashMapWithExpectedSize(values.size());
        // We are explicitly using a primitive for-loop (no streaming) to squeeze out a bit of performance
        // (same in decode)
        for (int i = 0; i < orderedKeys.size(); i++) {
            V value = values.get(orderedKeys.get(i));
            if (value != null) {
                res.put(i, valueMapper.apply(value));
            }
        }
        return res;
    }

    public static <K, V> Map<K, V> decode(List<K> orderedKeys, Map<Integer, V> indexEncodedValues) {
        return decode(orderedKeys, indexEncodedValues, Function.identity());
    }

    /**
     * Given an ordered list of keys and a map of indices to values, returns a map of keys to values.
     * The values are determined by indexing into the list of keys.
     * Values of the returned map are created using the provided mapping function.
     *
     * @param orderedKeys a list of keys in any fixed (!) order. This MUST be the same list that was used for
     * encoding.
     * @param indexEncodedValues a map of indices to values. Every index MUST be a valid index into the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     */
    public static <K, V, R> Map<K, R> decode(
            List<K> orderedKeys, Map<Integer, V> indexEncodedValues, Function<V, R> valueMapper) {
        Map<K, R> res = Maps.newHashMapWithExpectedSize(indexEncodedValues.size());
        for (Map.Entry<Integer, V> entry : indexEncodedValues.entrySet()) {
            res.put(orderedKeys.get(entry.getKey()), valueMapper.apply(entry.getValue()));
        }
        return res;
    }
}
