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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.common.streams.KeyedStream;
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
    private static final Function<Long, String> VALUE_MAPPER = value -> Long.toString(value - 10);
    private static final Function<String, Long> REVERSE_MAPPER = value -> Long.parseLong(value) + 10;

    @Test
    public void canEncodeSimpleData() {
        assertThat(IndexEncodingUtils.encode(ORDERED_KEYS, VALUES))
                .containsExactlyInAnyOrderEntriesOf(INDEX_ENCODED_VALUES);
    }

    @Test
    public void canDecodeSimpleData() {
        assertThat(IndexEncodingUtils.decode(ORDERED_KEYS, INDEX_ENCODED_VALUES))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void decodeAndEncodeAreEqualForRandomData() {
        Random rand = new Random();
        List<UUID> orderedKeys =
                IntStream.range(0, 1000).mapToObj(unused -> UUID.randomUUID()).collect(Collectors.toList());
        Map<UUID, Long> data = orderedKeys.stream().collect(Collectors.toMap(key -> key, key -> rand.nextLong()));
        Assertions.assertThat(IndexEncodingUtils.decode(orderedKeys, IndexEncodingUtils.encode(orderedKeys, data)))
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    @Test
    public void doesNotCloneObjects() {
        String key = "test-key";
        Pair<Long, String> value = Pair.create(10L, "test");
        List<String> orderedKeys = ImmutableList.of(key);
        Map<String, Pair<Long, String>> data = ImmutableMap.of(key, value);
        assertThat(IndexEncodingUtils.decode(orderedKeys, IndexEncodingUtils.encode(orderedKeys, data))
                        .get(key))
                .isSameAs(value);
    }

    @Test
    public void canEncodeWithValueMapper() {
        Map<Integer, String> expectedValues = KeyedStream.ofEntries(INDEX_ENCODED_VALUES.entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.encode(ORDERED_KEYS, VALUES, VALUE_MAPPER))
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @Test
    public void canDecodeWithValueMapper() {
        Map<Integer, String> indexEncodedValues = KeyedStream.ofEntries(INDEX_ENCODED_VALUES.entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.decode(ORDERED_KEYS, indexEncodedValues, REVERSE_MAPPER))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }
}
