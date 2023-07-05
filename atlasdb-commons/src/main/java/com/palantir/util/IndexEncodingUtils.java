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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.zip.CRC32;

public final class IndexEncodingUtils {
    private IndexEncodingUtils() {}

    /**
     * Given an ordered list of keys and a map of keys to values, compute a map that uses indices into the list of keys
     * instead of actual keys. Values of the returned map are created using the provided mapping function.
     * Also returns a checksum of the ordered keys by extracting bytes from the key objects using a provided function.
     *
     * @param keyList a list of keys in any fixed (!) order. The same list MUST be used for decoding
     * @param keyToValue a map of keys to values. Every key MUST be contained in the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     * @param byteMapper a function that extracts bytes from a key object, which are used to compute the checksum
     */
    public static <K, V, R> IndexEncodingWithChecksum<R> encode(
            List<K> keyList, Map<K, V> keyToValue, Function<V, R> valueMapper, Function<K, byte[]> byteMapper) {
        // A linked hash map will give a minor improvement when iterating during serialization
        Map<Integer, R> indexToValue = Maps.newLinkedHashMapWithExpectedSize(keyToValue.size());
        IntStream.range(0, keyList.size()).forEach(i -> {
            V value = keyToValue.get(keyList.get(i));
            if (value != null) {
                indexToValue.put(i, valueMapper.apply(value));
            }
        });
        for (int i = 0; i < keyList.size(); i++) {
            V value = keyToValue.get(keyList.get(i));
            if (value != null) {
                indexToValue.put(i, valueMapper.apply(value));
            }
        }
        return IndexEncodingWithChecksum.of(indexToValue, getCheckSumForKeys(keyList, byteMapper));
    }

    /**
     * Given an ordered list of keys and a map of indices to values, returns a map of keys to values.
     * The values are determined by indexing into the list of keys.
     * Values of the returned map are created using the provided mapping function.
     * Also computes a checksum of the ordered keys using a provided function and compares it to the provided checksum.
     *
     * @param keyList a list of keys in any fixed (!) order. This MUST be the same list that was used for
     * encoding.
     * @param indexEncodingWithChecksum a map of indices to values with a checksum of the ordered keys used to compute it.
     * Every index MUST be a valid index into the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     * @param byteMapper a function that extracts bytes from a key object, which are used to compute the checksum
     * @throws IllegalArgumentException if the provided checksum does not match the checksum of the ordered keys
     */
    public static <K, V, R> Map<K, R> decode(
            List<K> keyList,
            IndexEncodingWithChecksum<V> indexEncodingWithChecksum,
            Function<V, R> valueMapper,
            Function<K, byte[]> byteMapper)
            throws SafeIllegalArgumentException {
        Preconditions.checkArgument(
                getCheckSumForKeys(keyList, byteMapper) == indexEncodingWithChecksum.keyListCrc32Checksum(),
                "Key list integrity check failed");

        Map<Integer, V> indexEncodedValues = indexEncodingWithChecksum.indexToValue();
        Map<K, R> keyToValue = Maps.newHashMapWithExpectedSize(indexEncodedValues.size());
        for (Map.Entry<Integer, V> entry : indexEncodedValues.entrySet()) {
            keyToValue.put(keyList.get(entry.getKey()), valueMapper.apply(entry.getValue()));
        }
        return keyToValue;
    }

    private static <K> long getCheckSumForKeys(List<K> keyList, Function<K, byte[]> byteMapper) {
        CRC32 orderedKeyChecksum = new CRC32();
        for (K key : keyList) {
            orderedKeyChecksum.update(byteMapper.apply(key));
        }
        return orderedKeyChecksum.getValue();
    }
}
