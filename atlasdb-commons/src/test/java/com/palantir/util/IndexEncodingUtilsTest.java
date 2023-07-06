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
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeExceptions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.util.IndexEncodingUtils.ChecksumType;
import com.palantir.util.IndexEncodingUtils.DeterministicHashable;
import com.palantir.util.IndexEncodingUtils.IndexEncodingResult;
import com.palantir.util.IndexEncodingUtils.KeyListChecksum;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IndexEncodingUtilsTest {

    @Parameterized.Parameters(name = "checksumType={0}")
    public static Iterable<ChecksumType> data() {
        return Arrays.asList(ChecksumType.values());
    }

    private static final Map<DH<String>, Long> VALUES =
            ImmutableMap.of(dh("key1"), 42L, dh("key2"), 1337L, dh("anotherKey"), -10L);
    private static final Set<DH<String>> KEYS = ImmutableSet.of(dh("key1"), dh("key2"), dh("anotherKey"));
    private static final Map<Integer, Long> INDEX_ENCODED_VALUES = ImmutableMap.of(1, 1337L, 2, -10L, 0, 42L);
    private static final Function<Long, String> VALUE_MAPPER = value -> Long.toString(value - 10);
    private static final Function<String, Long> REVERSE_MAPPER = value -> Long.parseLong(value) + 10;

    private final ChecksumType checksumType;
    private IndexEncodingResult<DH<String>, Long> encoded;

    public IndexEncodingUtilsTest(ChecksumType checksumType) {
        this.checksumType = checksumType;
    }

    @Before
    public void setup() {
        encoded = IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), checksumType);
    }

    @Test
    public void canEncodeSimpleData() {
        assertThat(encoded.indexToValue()).isEqualTo(INDEX_ENCODED_VALUES);
    }

    @Test
    public void canEncodeAndDecodeSimpleData() {
        assertThat(IndexEncodingUtils.decode(encoded, Function.identity())).containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void canEncodeWithCustomValueMapper() {
        Map<Integer, String> expectedValues = KeyedStream.ofEntries(INDEX_ENCODED_VALUES.entrySet().stream())
                .map(VALUE_MAPPER)
                .collectToMap();
        assertThat(IndexEncodingUtils.encode(KEYS, VALUES, VALUE_MAPPER, checksumType)
                        .indexToValue())
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @Test
    public void canDecodeWithCustomValueMapper() {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(KEYS, VALUES, VALUE_MAPPER, checksumType), REVERSE_MAPPER))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void encodeChecksForUnknownKeysInValueMap() {
        assertThatException()
                .isThrownBy(() -> IndexEncodingUtils.encode(
                        KEYS, ImmutableMap.of(dh("unknown-key"), 0L), Function.identity(), checksumType))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .withMessage(SafeExceptions.renderMessage(
                        "keyToValue contains keys that are not in the key list",
                        UnsafeArg.of("unknownKeys", ImmutableSet.of(dh("unknown-key")))));
    }

    @Test
    public void integrityCheckFailsForDifferentKeyList() {
        List<DH<String>> modifiedKeyList = new ArrayList<>(KEYS);
        Collections.swap(modifiedKeyList, 0, 1);
        IndexEncodingResult<DH<String>, Long> encodedWithModifiedKeyList =
                IndexEncodingResult.of(modifiedKeyList, encoded.indexToValue(), encoded.keyListChecksum());
        KeyListChecksum actualChecksum = IndexEncodingUtils.computeChecksum(checksumType, modifiedKeyList);
        assertThatThrownBy(() -> IndexEncodingUtils.decode(encodedWithModifiedKeyList, Function.identity()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage(SafeExceptions.renderMessage(
                        "Key list integrity check failed",
                        UnsafeArg.of("keyList", modifiedKeyList),
                        UnsafeArg.of("actualChecksum", actualChecksum),
                        UnsafeArg.of("expectedChecksum", encoded.keyListChecksum())));
    }

    @Test
    public void integrityCheckFailsForDifferentChecksum() {
        byte[] modifiedChecksum = encoded.keyListChecksum().value();
        modifiedChecksum[0]++;
        IndexEncodingResult<DH<String>, Long> encodedWithModifiedChecksum = IndexEncodingResult.of(
                encoded.keyList(),
                encoded.indexToValue(),
                KeyListChecksum.of(encoded.keyListChecksum().type(), modifiedChecksum));
        assertThatThrownBy(() -> IndexEncodingUtils.decode(encodedWithModifiedChecksum, Function.identity()))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage(SafeExceptions.renderMessage(
                        "Key list integrity check failed",
                        UnsafeArg.of("keyList", KEYS),
                        UnsafeArg.of("actualChecksum", encoded.keyListChecksum()),
                        UnsafeArg.of("expectedChecksum", encodedWithModifiedChecksum.keyListChecksum())));
    }

    @Test
    public void decodeAndEncodeAreEqualForRandomData() {
        Random rand = new Random();
        Set<DH<UUID>> keys = Stream.generate(UUID::randomUUID)
                .limit(1000)
                .map(IndexEncodingUtilsTest::dh)
                .collect(Collectors.toSet());
        Map<DH<UUID>, Long> data = KeyedStream.of(keys.stream())
                .filter(unused -> rand.nextBoolean())
                .map(_unused -> rand.nextLong())
                .collectToMap();
        Assertions.assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(keys, data, Function.identity(), checksumType), Function.identity()))
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    // DH = deterministic hashable
    @Value.Immutable
    abstract static class DH<T> implements DeterministicHashable {

        @Value.Parameter
        protected abstract T delegate();

        @Override
        public int getDeterministicHashCode() {
            return delegate().hashCode();
        }
    }

    private static <T> DH<T> dh(T value) {
        return ImmutableDH.of(value);
    }
}
