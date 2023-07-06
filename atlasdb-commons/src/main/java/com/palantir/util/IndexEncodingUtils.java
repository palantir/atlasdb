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

import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.google.common.collect.Maps;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.CRC32;
import org.immutables.value.Value;

public final class IndexEncodingUtils {
    private IndexEncodingUtils() {}

    /**
     * Given a set of keys and a map of keys to values, we first order the keys in some fixed manner and then compute
     * a map that uses indices into this list of keys instead of actual keys.
     * Values of the returned map are created using the provided mapping function.
     * We return the list of keys, the index map, and a CRC32 checksum of the ordered keys.
     *
     * @param keys a collection of keys (must not be ordered)
     * @param keyToValue a map of keys to values. Every key MUST be contained in the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     * @param byteMapper a function that extracts bytes from a key object, which are used to compute the checksum
     * @throws SafeIllegalArgumentException if the value map contains keys that are not in the key list
     */
    public static <K, V, R> IndexEncodingResult<K, R> encode(
            Set<K> keys, Map<K, V> keyToValue, Function<V, R> valueMapper, Function<K, byte[]> byteMapper) {
        List<K> keyList = new ArrayList<>(keys);
        // A linked hash map will give a minor improvement when iterating during serialization
        Map<Integer, R> indexToValue = Maps.newLinkedHashMapWithExpectedSize(keyToValue.size());
        // We are explicitly using a primitive for-loop (no streaming) to squeeze out a bit of performance
        // (same in decode)
        for (int i = 0; i < keyList.size(); i++) {
            V value = keyToValue.get(keyList.get(i));
            if (value != null) {
                indexToValue.put(i, valueMapper.apply(value));
            }
        }
        Preconditions.checkArgument(
                indexToValue.size() >= keyToValue.size(), "Value map contains keys that are not in the key list");
        return IndexEncodingResult.of(keyList, indexToValue, computeCheckSum(keyList, byteMapper));
    }

    /**
     * Given an ordered list of keys and a map of indices to values, returns a map of keys to values.
     * The values are determined by indexing into the list of keys.
     * Values of the returned map are created using the provided mapping function.
     * Also computes a checksum of the ordered keys using a provided function and compares it to the provided checksum.
     *
     * @param indexEncodingResult the output of {@link IndexEncodingUtils#encode}, i.e. the ordered list of keys,
     * a map of indices to values, and a checksum of the ordered key list used to compute it. Every index MUST be
     * a valid index into the list of keys.
     * @param valueMapper a mapping function to run before putting a value into the result map
     * @param byteMapper a function that extracts bytes from a key object, which are used to compute the checksum
     * @throws SafeIllegalArgumentException if the provided checksum does not match the checksum of the ordered keys
     */
    public static <K, V, R> Map<K, R> decode(
            IndexEncodingResult<K, V> indexEncodingResult, Function<V, R> valueMapper, Function<K, byte[]> byteMapper) {
        Preconditions.checkArgument(
                computeCheckSum(indexEncodingResult.keyList(), byteMapper)
                        == indexEncodingResult.keyListCrc32Checksum(),
                "Key list integrity check failed");

        Map<Integer, V> indexToValue = indexEncodingResult.indexToValue();
        Map<K, R> keyToValue = Maps.newHashMapWithExpectedSize(indexToValue.size());
        for (Map.Entry<Integer, V> entry : indexToValue.entrySet()) {
            keyToValue.put(indexEncodingResult.keyList().get(entry.getKey()), valueMapper.apply(entry.getValue()));
        }
        return keyToValue;
    }

    private static <K> long computeCheckSum(List<K> keyList, Function<K, byte[]> byteMapper) {
        CRC32 orderedKeyChecksum = new CRC32();
        for (K key : keyList) {
            orderedKeyChecksum.update(byteMapper.apply(key));
        }
        return orderedKeyChecksum.getValue();
    }

    /**
     * This class is merely used to wrap the output of {@link IndexEncodingUtils#encode} and should not be embedded in
     * any other object directly or serialized as-is.
     */
    @Value.Immutable
    @JsonIgnoreType
    public interface IndexEncodingResult<K, V> {
        @Value.Parameter
        List<K> keyList();

        @Value.Parameter
        Map<Integer, V> indexToValue();

        @Value.Parameter
        long keyListCrc32Checksum();

        static <K, V> IndexEncodingResult<K, V> of(
                List<K> keyList, Map<Integer, V> indexToValue, long keyListCrc32Checksum) {
            return ImmutableIndexEncodingResult.of(keyList, indexToValue, keyListCrc32Checksum);
        }
    }
}
