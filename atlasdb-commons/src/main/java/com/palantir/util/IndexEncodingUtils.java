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
import com.google.common.collect.Sets;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
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
     * Compute a derived map, replacing keys with their associated index into the ordered list, to the value returned
     * by running the valueMapper over the original value.
     * If the original values are transmitted as associated objects for some keys and keys can be large,
     * this encoding can be used to save significant space on the wire.
     *
     * @param keys a set of keys
     * @param keyToValue a map of keys to values. Every key in this map MUST be contained in the set of keys.
     * @param valueMapper a mapping function applied to values before placing them into the result map
     * @param checksumType the type of checksum algorithm to use
     * @return the list of keys, the index map, and a compound checksum of the ordered keys.
     * @throws SafeIllegalArgumentException if the {@code keyToValue} contains keys that are not in the provided
     * set of keys
     */
    public static <K, V, R> IndexEncodingResult<K, R> encode(
            Set<K> keys, Map<K, V> keyToValue, Function<V, R> valueMapper, ChecksumType checksumType) {
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
        if (indexToValue.size() != keyToValue.size()) {
            Set<K> unknownKeys = Sets.difference(keyToValue.keySet(), keys);
            throw new SafeIllegalArgumentException(
                    "keyToValue contains keys that are not in the key list", UnsafeArg.of("unknownKeys", unknownKeys));
        }
        return IndexEncodingResult.of(keyList, indexToValue, computeChecksum(checksumType, keyList));
    }

    /**
     * Compute a derived map, replacing indices into the ordered list with their items, to the value returned
     * by running the valueMapper over the original value.
     *
     * @param indexEncodingResult the output of {@link IndexEncodingUtils#encode}, i.e. the ordered list of keys,
     * a map of indices to values, and a checksum of the ordered key list. Every index MUST be
     * a valid index into the list of keys.
     * @param valueMapper a mapping function applied to values before placing them into the result map
     * @throws SafeIllegalArgumentException if the provided checksum does not match the checksum of the ordered keys
     */
    public static <K, V, R> Map<K, R> decode(
            IndexEncodingResult<K, V> indexEncodingResult, Function<V, R> valueMapper) {
        Preconditions.checkArgument(
                computeChecksum(indexEncodingResult.keyListChecksum().checksumType(), indexEncodingResult.keyList())
                        .equals(indexEncodingResult.keyListChecksum()),
                "Key list integrity check failed");

        Map<Integer, V> indexToValue = indexEncodingResult.indexToValue();
        Map<K, R> keyToValue = Maps.newHashMapWithExpectedSize(indexToValue.size());
        for (Map.Entry<Integer, V> entry : indexToValue.entrySet()) {
            keyToValue.put(indexEncodingResult.keyList().get(entry.getKey()), valueMapper.apply(entry.getValue()));
        }
        return keyToValue;
    }

    private static <K> KeyListChecksum computeChecksum(ChecksumType checksumType, List<K> keyList) {
        byte[] checksumValue;
        switch (checksumType) {
            case LIST_HASHCODE: {
                checksumValue =
                        ByteBuffer.allocate(4).putInt(keyList.hashCode()).array();
                break;
            }
            case CRC32_OF_ITEM_HASHCODE: {
                CRC32 orderedKeyChecksum = new CRC32();
                for (K key : keyList) {
                    orderedKeyChecksum.update(key.hashCode());
                }
                checksumValue = ByteBuffer.allocate(8)
                        .putLong(orderedKeyChecksum.getValue())
                        .array();
                break;
            }
            default: {
                throw new IllegalArgumentException("Unknown checksum type: " + checksumType);
            }
        }
        return KeyListChecksum.of(checksumType, checksumValue);
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
        KeyListChecksum keyListChecksum();

        static <K, V> IndexEncodingResult<K, V> of(
                List<K> keyList, Map<Integer, V> indexToValue, KeyListChecksum keyListChecksum) {
            return ImmutableIndexEncodingResult.of(keyList, indexToValue, keyListChecksum);
        }
    }

    public enum ChecksumType {
        LIST_HASHCODE,
        CRC32_OF_ITEM_HASHCODE
    }

    @Value.Immutable
    public interface KeyListChecksum {

        @Value.Parameter
        ChecksumType checksumType();

        @Value.Parameter
        byte[] checksumValue();

        static KeyListChecksum of(ChecksumType checksumType, byte[] checksumValue) {
            return ImmutableKeyListChecksum.of(checksumType, checksumValue);
        }
    }
}
