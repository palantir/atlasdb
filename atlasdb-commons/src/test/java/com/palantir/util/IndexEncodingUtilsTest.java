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
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class IndexEncodingUtilsTest {
    private static final Map<String, Long> VALUES = ImmutableMap.of("key1", 42L, "key2", 1337L, "anotherKey", -10L);
    private static final Set<String> KEYS = ImmutableSet.of("key1", "key2", "anotherKey");
    private static final Map<Integer, Long> INDEX_ENCODED_VALUES = ImmutableMap.of(1, 1337L, 2, -10L, 0, 42L);
    private static final Function<Long, String> VALUE_MAPPER = value -> Long.toString(value - 10);
    private static final Function<String, Long> REVERSE_MAPPER = value -> Long.parseLong(value) + 10;
    private static final Function<UUID, byte[]> UUID_BYTE_MAPPER = value -> ByteBuffer.allocate(16)
            .putLong(0, value.getMostSignificantBits())
            .putLong(8, value.getLeastSignificantBits())
            .array();

    @Test
    public void canEncodeSimpleData() {
        assertThat(IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), IndexEncodingUtilsTest::dummyByteMapper)
                        .indexToValue())
                .isEqualTo(INDEX_ENCODED_VALUES);
    }

    @Test
    public void canEncodeAndDecodeSimpleData() {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(
                                KEYS, VALUES, Function.identity(), IndexEncodingUtilsTest::dummyByteMapper),
                        Function.identity(),
                        IndexEncodingUtilsTest::dummyByteMapper))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void canEncodeWithCustomValueMapper() {
        Map<Integer, String> expectedValues = KeyedStream.ofEntries(INDEX_ENCODED_VALUES.entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.encode(KEYS, VALUES, VALUE_MAPPER, IndexEncodingUtilsTest::dummyByteMapper)
                        .indexToValue())
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @Test
    public void canDecodeWithCustomValueMapper() {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(KEYS, VALUES, VALUE_MAPPER, IndexEncodingUtilsTest::dummyByteMapper),
                        REVERSE_MAPPER,
                        IndexEncodingUtilsTest::dummyByteMapper))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void encodeChecksForUnknownKeysInValueMap() {
        assertThatException()
                .isThrownBy(() -> IndexEncodingUtils.encode(
                        KEYS,
                        ImmutableMap.of("unknown-key", 0L),
                        Function.identity(),
                        IndexEncodingUtilsTest::dummyByteMapper))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .withMessage("Value map contains keys that are not in the key list");
    }

    @Test
    public void integrityCheckPassesForSameKeyList() {
        IndexEncodingResult<String, Long> encoded =
                IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), String::getBytes);
        Map<String, Long> decoded = IndexEncodingUtils.decode(encoded, Function.identity(), String::getBytes);
        assertThat(decoded).containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void integrityCheckFailsForDifferentKeyList() {
        IndexEncodingResult<String, Long> encoded =
                IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), String::getBytes);
        List<String> modifiedKeyList = new ArrayList<>(KEYS);
        Collections.swap(modifiedKeyList, 0, 1);
        IndexEncodingResult<String, Long> encodedWithModifiedKeyList =
                IndexEncodingResult.of(modifiedKeyList, encoded.indexToValue(), encoded.keyListCrc32Checksum());
        assertThatThrownBy(() ->
                        IndexEncodingUtils.decode(encodedWithModifiedKeyList, Function.identity(), String::getBytes))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Key list integrity check failed");
    }

    @Test
    public void integrityCheckFailsForDifferentChecksum() {
        IndexEncodingResult<String, Long> encoded =
                IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), String::getBytes);
        IndexEncodingResult<String, Long> encodedWithModifiedChecksum =
                IndexEncodingResult.of(encoded.keyList(), encoded.indexToValue(), encoded.keyListCrc32Checksum() + 1);
        assertThatThrownBy(() ->
                        IndexEncodingUtils.decode(encodedWithModifiedChecksum, Function.identity(), String::getBytes))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Key list integrity check failed");
    }

    @Test
    public void decodeAndEncodeAreEqualForRandomData() {
        Random rand = new Random();
        Set<UUID> keys = Stream.generate(UUID::randomUUID).limit(1000).collect(Collectors.toSet());
        Map<UUID, Long> data = KeyedStream.of(keys.stream())
                .filter(unused -> rand.nextBoolean())
                .map(_unused -> rand.nextLong())
                .collectToMap();
        Assertions.assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(keys, data, Function.identity(), UUID_BYTE_MAPPER),
                        Function.identity(),
                        UUID_BYTE_MAPPER))
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    private static <K> byte[] dummyByteMapper(K key) {
        return new byte[0];
    }
}
