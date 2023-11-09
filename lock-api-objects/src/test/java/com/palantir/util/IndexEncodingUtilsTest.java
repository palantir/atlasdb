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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class IndexEncodingUtilsTest {

    public static List<ChecksumType> getParameters() {
        return Arrays.asList(ChecksumType.values());
    }

    private static final Map<DeterministicHashableWrapper<String>, Long> VALUES =
            ImmutableMap.of(wrap("key1"), 42L, wrap("key2"), 1337L, wrap("anotherKey"), -10L);
    private static final Set<DeterministicHashableWrapper<String>> KEYS =
            ImmutableSet.of(wrap("key1"), wrap("key2"), wrap("anotherKey"));

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canLookupValidChecksumType(ChecksumType checksumType) {
        assertThat(ChecksumType.valueOf(checksumType.getId())).isEqualTo(checksumType);
    }

    @Test
    public void cannotLookupUnknownChecksumType() {
        assertThatLoggableExceptionThrownBy(() -> ChecksumType.valueOf(-1))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Unknown checksum type ID")
                .hasExactlyArgs(SafeArg.of("checksumTypeId", -1));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canEncodeSimpleData(ChecksumType checksumType) {
        assertThat(createIndexEncoding(checksumType).indexToValue())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(1, 1337L, 2, -10L, 0, 42L));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canDecodeSimpleData(ChecksumType checksumType) {
        assertThat(IndexEncodingUtils.decode(createIndexEncoding(checksumType)))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canEncodeAndDecodeEmptyData(ChecksumType checksumType) {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(ImmutableSet.of(), ImmutableMap.of(), checksumType)))
                .isEmpty();
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canEncodeWithCustomValueMapper(ChecksumType checksumType) {
        Map<Integer, String> expectedValues = ImmutableMap.of(1, "1327", 2, "-20", 0, "32");
        assertThat(IndexEncodingUtils.encode(KEYS, VALUES, value -> Long.toString(value - 10), checksumType)
                        .indexToValue())
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void canDecodeWithCustomValueMapper(ChecksumType checksumType) {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(KEYS, VALUES, value -> Long.toString(value - 10), checksumType),
                        value -> Long.parseLong(value) + 10))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void encodeFailsForUnknownKeysInValueMap(ChecksumType checksumType) {
        assertThatLoggableExceptionThrownBy(
                        () -> IndexEncodingUtils.encode(KEYS, ImmutableMap.of(wrap("unknown-key"), 0L), checksumType))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Value map uses keys that are not in the provided set of keys")
                .hasExactlyArgs(UnsafeArg.of("unknownKeys", ImmutableSet.of(wrap("unknown-key"))));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void decodeFailsForInvalidIndices(ChecksumType checksumType) {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithInvalidIndices =
                ImmutableIndexEncodingResult.copyOf(createIndexEncoding(checksumType))
                        .withIndexToValue(ImmutableMap.of(KEYS.size(), 17L));
        assertThatLoggableExceptionThrownBy(() -> IndexEncodingUtils.decode(encodedWithInvalidIndices))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Index map contains invalid index")
                .hasExactlyArgs(SafeArg.of("index", KEYS.size()), SafeArg.of("keyListSize", KEYS.size()));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void integrityCheckFailsForDifferentKeyList(ChecksumType checksumType) {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encoded = createIndexEncoding(checksumType);
        List<DeterministicHashableWrapper<String>> modifiedKeyList = new ArrayList<>(KEYS);
        Collections.swap(modifiedKeyList, 0, 1);
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithModifiedKeyList =
                ImmutableIndexEncodingResult.copyOf(encoded).withKeyList(modifiedKeyList);
        KeyListChecksum actualChecksum = IndexEncodingUtils.computeChecksum(checksumType, modifiedKeyList);
        assertThatLoggableExceptionThrownBy(() -> IndexEncodingUtils.decode(encodedWithModifiedKeyList))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Key list integrity check failed")
                .hasExactlyArgs(
                        UnsafeArg.of("keyList", modifiedKeyList),
                        SafeArg.of("actualChecksum", actualChecksum),
                        SafeArg.of("expectedChecksum", encoded.keyListChecksum()));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void integrityCheckFailsForDifferentChecksum(ChecksumType checksumType) {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encoded = createIndexEncoding(checksumType);
        byte[] modifiedChecksum = encoded.keyListChecksum().value();
        modifiedChecksum[0]++;
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithModifiedChecksum =
                ImmutableIndexEncodingResult.copyOf(encoded)
                        .withKeyListChecksum(KeyListChecksum.of(checksumType, modifiedChecksum));
        assertThatLoggableExceptionThrownBy(() -> IndexEncodingUtils.decode(encodedWithModifiedChecksum))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Key list integrity check failed")
                .hasExactlyArgs(
                        UnsafeArg.of("keyList", encoded.keyList()),
                        SafeArg.of("actualChecksum", encoded.keyListChecksum()),
                        SafeArg.of("expectedChecksum", encodedWithModifiedChecksum.keyListChecksum()));
    }

    @ParameterizedTest(name = "checksumType={0}")
    @MethodSource("getParameters")
    public void decodingEncodedDataYieldsOriginalForRandomData(ChecksumType checksumType) {
        int seed = (int) System.currentTimeMillis();
        Random rand = new Random(seed);
        Set<DeterministicHashableWrapper<UUID>> keys = Stream.generate(UUID::randomUUID)
                .limit(1000)
                .map(IndexEncodingUtilsTest::wrap)
                .collect(Collectors.toSet());
        Map<DeterministicHashableWrapper<UUID>, Long> data = KeyedStream.of(keys.stream())
                .filter(_unused -> rand.nextBoolean())
                .map(_unused -> rand.nextLong())
                .collectToMap();
        assertThat(IndexEncodingUtils.decode(IndexEncodingUtils.encode(keys, data, checksumType)))
                .as("Encoding and decoding yields original data. Random seed: " + seed)
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    private IndexEncodingResult<DeterministicHashableWrapper<String>, Long> createIndexEncoding(
            ChecksumType checksumType) {
        return IndexEncodingUtils.encode(KEYS, VALUES, checksumType);
    }

    private static <T> DeterministicHashableWrapper<T> wrap(T value) {
        return ImmutableDeterministicHashableWrapper.of(value);
    }

    @Value.Immutable
    abstract static class DeterministicHashableWrapper<T> implements DeterministicHashable {

        @Value.Parameter
        protected abstract T delegate();

        @Override
        public int deterministicHashCode() {
            return delegate().hashCode();
        }
    }
}
