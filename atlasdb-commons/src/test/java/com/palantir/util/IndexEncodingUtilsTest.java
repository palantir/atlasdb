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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.immutables.value.Value;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class IndexEncodingUtilsTest {

    @Parameterized.Parameters(name = "checksumType={0}")
    public static Iterable<ChecksumType> data() {
        return Arrays.asList(ChecksumType.values());
    }

    private static final Map<DeterministicHashableWrapper<String>, Long> VALUES =
            ImmutableMap.of(wrap("key1"), 42L, wrap("key2"), 1337L, wrap("anotherKey"), -10L);
    private static final Set<DeterministicHashableWrapper<String>> KEYS =
            ImmutableSet.of(wrap("key1"), wrap("key2"), wrap("anotherKey"));

    private final ChecksumType checksumType;

    public IndexEncodingUtilsTest(ChecksumType checksumType) {
        this.checksumType = checksumType;
    }

    @Test
    public void canLookupValidChecksumType() {
        assertThat(ChecksumType.valueOf(checksumType.getId())).isEqualTo(checksumType);
    }

    @Test
    public void cannotLookupUnknownChecksumType() {
        assertThatLoggableExceptionThrownBy(() -> ChecksumType.valueOf(-1))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Unknown checksum type ID")
                .hasExactlyArgs(SafeArg.of("checksumTypeId", -1));
    }

    @Test
    public void canEncodeSimpleData() {
        assertThat(createIndexEncodingWithIdentity().indexToValue())
                .isEqualTo(ImmutableMap.of(1, 1337L, 2, -10L, 0, 42L));
    }

    @Test
    public void canDecodeSimpleData() {
        assertThat(IndexEncodingUtils.decode(createIndexEncodingWithIdentity(), Function.identity()))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void canEncodeAndDecodeEmptyData() {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(
                                ImmutableSet.of(), ImmutableMap.of(), Function.identity(), checksumType),
                        Function.identity()))
                .isEmpty();
    }

    @Test
    public void canEncodeWithCustomValueMapper() {
        Map<Integer, String> expectedValues = ImmutableMap.of(1, "1327", 2, "-20", 0, "32");
        assertThat(IndexEncodingUtils.encode(KEYS, VALUES, value -> Long.toString(value - 10), checksumType)
                        .indexToValue())
                .containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    @Test
    public void canDecodeWithCustomValueMapper() {
        assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(KEYS, VALUES, value -> Long.toString(value - 10), checksumType),
                        value -> Long.parseLong(value) + 10))
                .containsExactlyInAnyOrderEntriesOf(VALUES);
    }

    @Test
    public void encodeFailsForUnknownKeysInValueMap() {
        assertThatLoggableExceptionThrownBy(() -> IndexEncodingUtils.encode(
                        KEYS, ImmutableMap.of(wrap("unknown-key"), 0L), Function.identity(), checksumType))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Value map uses keys that are not in the provided set of keys")
                .hasExactlyArgs(UnsafeArg.of("unknownKeys", ImmutableSet.of(wrap("unknown-key"))));
    }

    @Test
    public void decodeFailsForInvalidIndices() {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithInvalidIndices =
                ImmutableIndexEncodingResult.copyOf(createIndexEncodingWithIdentity())
                        .withIndexToValue(ImmutableMap.of(KEYS.size(), 17L));
        assertThatLoggableExceptionThrownBy(
                        () -> IndexEncodingUtils.decode(encodedWithInvalidIndices, Function.identity()))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Index map contains invalid index")
                .hasExactlyArgs(SafeArg.of("index", KEYS.size()), SafeArg.of("keyListSize", KEYS.size()));
    }

    @Test
    public void integrityCheckFailsForDifferentKeyList() {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encoded = createIndexEncodingWithIdentity();
        List<DeterministicHashableWrapper<String>> modifiedKeyList = new ArrayList<>(KEYS);
        Collections.swap(modifiedKeyList, 0, 1);
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithModifiedKeyList =
                ImmutableIndexEncodingResult.copyOf(encoded).withKeyList(modifiedKeyList);
        KeyListChecksum actualChecksum = IndexEncodingUtils.computeChecksum(checksumType, modifiedKeyList);
        assertThatLoggableExceptionThrownBy(
                        () -> IndexEncodingUtils.decode(encodedWithModifiedKeyList, Function.identity()))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Key list integrity check failed")
                .hasExactlyArgs(
                        UnsafeArg.of("keyList", modifiedKeyList),
                        SafeArg.of("actualChecksum", actualChecksum),
                        SafeArg.of("expectedChecksum", encoded.keyListChecksum()));
    }

    @Test
    public void integrityCheckFailsForDifferentChecksum() {
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encoded = createIndexEncodingWithIdentity();
        byte[] modifiedChecksum = encoded.keyListChecksum().value();
        modifiedChecksum[0]++;
        IndexEncodingResult<DeterministicHashableWrapper<String>, Long> encodedWithModifiedChecksum =
                ImmutableIndexEncodingResult.copyOf(encoded)
                        .withKeyListChecksum(KeyListChecksum.of(checksumType, modifiedChecksum));
        assertThatLoggableExceptionThrownBy(
                        () -> IndexEncodingUtils.decode(encodedWithModifiedChecksum, Function.identity()))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Key list integrity check failed")
                .hasExactlyArgs(
                        UnsafeArg.of("keyList", encoded.keyList()),
                        SafeArg.of("actualChecksum", encoded.keyListChecksum()),
                        SafeArg.of("expectedChecksum", encodedWithModifiedChecksum.keyListChecksum()));
    }

    @Test
    public void decodingEncodedDataYieldsOriginalForRandomData() {
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
        Assertions.assertThat(IndexEncodingUtils.decode(
                        IndexEncodingUtils.encode(keys, data, Function.identity(), checksumType), Function.identity()))
                .as("Encoding and decoding yields original data. Random seed: " + seed)
                .containsExactlyInAnyOrderEntriesOf(data);
    }

    private IndexEncodingResult<DeterministicHashableWrapper<String>, Long> createIndexEncodingWithIdentity() {
        return IndexEncodingUtils.encode(KEYS, VALUES, Function.identity(), checksumType);
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
