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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class IndexEncodingUtilsTest {
    private static final Map<String, Long> VALUES = ImmutableMap.of("key1", 42L, "key2", 1337L, "anotherKey", -10L);
    private static final List<String> ORDERED_KEYS = ImmutableList.of("key1", "key2", "anotherKey");
    private static final Map<Integer, Long> INDEX_ENCODED_VALUES = ImmutableMap.of(1, 1337L, 2, -10L, 0, 42L);
    private static final long DUMMY_CHECKSUM = 0;
    private static final IndexEncodingWithChecksum<Long> INDEX_ENCODED_VALUES_WITH_DUMMY_CHECKSUM =
            IndexEncodingWithChecksum.of(INDEX_ENCODED_VALUES, DUMMY_CHECKSUM);
    private static final Function<Long, String> VALUE_MAPPER = value -> Long.toString(value - 10);
    private static final Function<String, Long> REVERSE_MAPPER = value -> Long.parseLong(value) + 10;

    @Test
    public void canEncodeSimpleData() {
        assertThat(IndexEncodingUtils.encode(
                                ORDERED_KEYS, VALUES, Function.identity(), IndexEncodingUtilsTest::dummyByteMapper)
                        .indexToValue())
                .isEqualTo(INDEX_ENCODED_VALUES);
    }

    @Test
    public void canDecodeSimpleData() {
        assertThat(IndexEncodingUtils.decode(
                        ORDERED_KEYS,
                        INDEX_ENCODED_VALUES_WITH_DUMMY_CHECKSUM,
                        Function.identity(),
                        IndexEncodingUtilsTest::dummyByteMapper))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void canEncodeWithCustomValueMapper() {
        Map<Integer, String> expectedValues = KeyedStream.ofEntries(
                        INDEX_ENCODED_VALUES_WITH_DUMMY_CHECKSUM.indexToValue().entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.encode(
                                ORDERED_KEYS, VALUES, VALUE_MAPPER, IndexEncodingUtilsTest::dummyByteMapper)
                        .indexToValue())
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @Test
    public void canDecodeWithCustomValueMapper() {
        Map<Integer, String> indexEncodedValues = KeyedStream.ofEntries(INDEX_ENCODED_VALUES.entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.decode(
                        ORDERED_KEYS,
                        IndexEncodingWithChecksum.of(indexEncodedValues, DUMMY_CHECKSUM),
                        REVERSE_MAPPER,
                        IndexEncodingUtilsTest::dummyByteMapper))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void integrityCheckPassesForSameKeyList() {
        IndexEncodingWithChecksum<Long> encoded =
                IndexEncodingUtils.encode(ORDERED_KEYS, VALUES, Function.identity(), String::getBytes);
        Map<String, Long> decoded =
                IndexEncodingUtils.decode(ORDERED_KEYS, encoded, Function.identity(), String::getBytes);
        assertThat(decoded).containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void integrityCheckFailsForDifferentKeyList() {
        IndexEncodingWithChecksum<Long> encoded =
                IndexEncodingUtils.encode(ORDERED_KEYS, VALUES, Function.identity(), String::getBytes);
        List<String> modifiedKeyList = new ArrayList<>(ORDERED_KEYS);
        Collections.swap(modifiedKeyList, 0, 1);
        assertThatThrownBy(() ->
                        IndexEncodingUtils.decode(modifiedKeyList, encoded, Function.identity(), String::getBytes))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Key list integrity check failed");
    }

    @Test
    public void integrityCheckFailsForDifferentChecksum() {
        IndexEncodingWithChecksum<Long> encoded =
                IndexEncodingUtils.encode(ORDERED_KEYS, VALUES, Function.identity(), String::getBytes);
        IndexEncodingWithChecksum<Long> encodedWithModifiedChecksum =
                IndexEncodingWithChecksum.of(encoded.indexToValue(), encoded.keyListCrc32Checksum() + 1);
        assertThatThrownBy(() -> IndexEncodingUtils.decode(
                        ORDERED_KEYS, encodedWithModifiedChecksum, Function.identity(), String::getBytes))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Key list integrity check failed");
    }

    @Test
    public void decodeAndEncodeAreEqualForRandomData() {
        Random rand = new Random();
        List<UUID> orderedKeys =
                IntStream.range(0, 1000).mapToObj(unused -> UUID.randomUUID()).collect(Collectors.toList());
        Map<UUID, Long> data = orderedKeys.stream().collect(Collectors.toMap(key -> key, key -> rand.nextLong()));
        Function<UUID, byte[]> byteMapper = value -> ByteBuffer.allocate(16)
                .putLong(0, value.getMostSignificantBits())
                .putLong(8, value.getLeastSignificantBits())
                .array();
        Assertions.assertThat(IndexEncodingUtils.decode(
                        orderedKeys,
                        IndexEncodingUtils.encode(orderedKeys, data, Function.identity(), byteMapper),
                        Function.identity(),
                        byteMapper))
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    private static <K> byte[] dummyByteMapper(K key) {
        return new byte[0];
    }
}
