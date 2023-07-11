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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.immutables.value.Value;

public final class IndexEncodingUtils {
    private IndexEncodingUtils() {}

    /**
     * Compute a derived map (replacing keys with their associated index into the ordered list) to the value returned
     * by running the {@code valueMapper} over the original value.
     * If the {@code keys} are transmitted regardless of whether they have a value associated in {@code keyToValue},
     * this encoding can be used to save significant space on the wire.
     *
     * @param keys a set of keys which implement {@link DeterministicHashable}
     * @param keyToValue a map of keys to values. The key set of this map must be a subset of {@code keys}
     * @param valueMapper a mapping function applied to values before placing them into the result map
     * @param checksumType the type of checksum algorithm to use on the ordered list of keys
     * @return the ordered list of keys, the index map, and a compound checksum of the ordered keys
     * @throws SafeIllegalArgumentException if {@code keyToValue} contains keys that are not in the provided
     * set of keys
     */
    public static <K extends DeterministicHashable, V, R> IndexEncodingResult<K, R> encode(
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
                    "Value map uses keys that are not in the provided set of keys",
                    UnsafeArg.of("unknownKeys", unknownKeys));
        }
        return IndexEncodingResult.<K, R>builder()
                .keyList(keyList)
                .indexToValue(indexToValue)
                .keyListChecksum(computeChecksum(checksumType, keyList))
                .build();
    }

    /**
     * Compute a derived map (replacing indices into the list of keys with their item) to the value returned
     * by running the {@code valueMapper} over the original value.
     *
     * @param indexEncoding the output of {@link IndexEncodingUtils#encode}, i.e. the ordered list of keys,
     * a map of indices to values, and a checksum of the ordered key list. Every index must be
     * a valid index into the list of keys
     * @param valueMapper a mapping function applied to values before placing them into the result map
     * @throws SafeIllegalArgumentException if the provided checksum does not match the actual checksum of the ordered
     * keys or if {@code indexToValue} contains indices that are not valid for the list of keys
     */
    public static <K extends DeterministicHashable, V, R> Map<K, R> decode(
            IndexEncodingResult<K, V> indexEncoding, Function<V, R> valueMapper) {
        List<K> keyList = indexEncoding.keyList();
        KeyListChecksum expectedChecksum = indexEncoding.keyListChecksum();
        KeyListChecksum actualChecksum = computeChecksum(expectedChecksum.type(), keyList);
        Preconditions.checkArgument(
                actualChecksum.equals(expectedChecksum),
                "Key list integrity check failed",
                UnsafeArg.of("keyList", keyList),
                SafeArg.of("actualChecksum", actualChecksum),
                SafeArg.of("expectedChecksum", expectedChecksum));

        Map<K, R> keyToValue =
                Maps.newHashMapWithExpectedSize(indexEncoding.indexToValue().size());
        for (Map.Entry<Integer, V> entry : indexEncoding.indexToValue().entrySet()) {
            int index = entry.getKey();
            Preconditions.checkArgument(
                    index >= 0 && index < keyList.size(),
                    "Index map contains invalid index",
                    SafeArg.of("index", index),
                    SafeArg.of("keyListSize", keyList.size()));
            keyToValue.put(keyList.get(index), valueMapper.apply(entry.getValue()));
        }
        return keyToValue;
    }

    @VisibleForTesting
    static <K extends DeterministicHashable> KeyListChecksum computeChecksum(
            ChecksumType checksumType, List<K> keyList) {
        switch (checksumType) {
            case CRC32_OF_DETERMINISTIC_HASHCODE: {
                CRC32 checksum = new CRC32();
                for (K key : keyList) {
                    checksum.update(key.deterministicHashCode());
                }
                return KeyListChecksum.of(
                        checksumType,
                        ByteBuffer.allocate(8).putLong(checksum.getValue()).array());
            }
            default: {
                throw new SafeIllegalArgumentException(
                        "Unknown checksum type", SafeArg.of("checksumType", checksumType));
            }
        }
    }

    public interface DeterministicHashable {

        /**
         * A hash code implementation that is stable across different processes/JVMs and only depends on the contents
         * of the object.
         */
        int deterministicHashCode();
    }

    /**
     * This class is merely used to wrap the output of {@link IndexEncodingUtils#encode} and should not be embedded in
     * any other object directly or serialized as-is.
     */
    @Value.Immutable
    @JsonIgnoreType
    public interface IndexEncodingResult<K extends DeterministicHashable, V> {
        List<K> keyList();

        KeyListChecksum keyListChecksum();

        Map<Integer, V> indexToValue();

        static <K extends DeterministicHashable, V> ImmutableIndexEncodingResult.Builder<K, V> builder() {
            return ImmutableIndexEncodingResult.builder();
        }
    }

    public enum ChecksumType {
        CRC32_OF_DETERMINISTIC_HASHCODE(1);

        private static final Map<Integer, ChecksumType> ID_TO_ENTRY =
                Arrays.stream(ChecksumType.values()).collect(Collectors.toMap(entry -> entry.id, entry -> entry));

        private final int id;

        ChecksumType(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public static ChecksumType valueOf(int id) {
            Preconditions.checkArgument(
                    ID_TO_ENTRY.containsKey(id), "Unknown checksum type ID", SafeArg.of("checksumTypeId", id));
            return ID_TO_ENTRY.get(id);
        }
    }

    @Value.Immutable
    public interface KeyListChecksum {

        @Value.Parameter
        ChecksumType type();

        @Value.Parameter
        byte[] value();

        static KeyListChecksum of(ChecksumType type, byte[] value) {
            return ImmutableKeyListChecksum.of(type, value);
        }
    }
}
