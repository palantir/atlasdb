/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.ptobject;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.ptobject.EncodingUtils.EncodingType;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.util.crypto.Sha256Hash;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.Test;

@SuppressWarnings("checkstyle:all")
public class EncodingUtilsTest {
    public final Random rand = new Random();

    @Test
    public void testBasicFlipBits() {
        byte[] ff = {-1};
        byte[] oo = {0};
        byte[] ffFlipped = EncodingUtils.flipAllBits(ff);
        byte[] ooFlipped = EncodingUtils.flipAllBits(oo);

        assertThat(ffFlipped).isEqualTo(oo);
        assertThat(ooFlipped).isEqualTo(ff);
    }

    @Test
    public void testFlipBits() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 100; i++) {
            rand.nextBytes(bytes);
            assertThat(EncodingUtils.flipAllBits(EncodingUtils.flipAllBits(bytes)))
                    .isEqualTo(bytes);
        }
    }

    @Test
    public void testVarString() throws Exception {
        for (int i = 1; i < 100; i++) {
            byte[] bytes = new byte[1000];
            rand.nextBytes(bytes);
            String str = new String(bytes);
            assertThat(EncodingUtils.decodeVarString(EncodingUtils.encodeVarString(str))
                            .getBytes())
                    .isEqualTo(str.getBytes());
            assertThat(EncodingUtils.encodeVarString(str)).hasSize(EncodingUtils.sizeOfVarString(str));
        }
    }

    @Test
    public void testVarSimple() {
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(Long.MIN_VALUE)))
                .isEqualTo(Long.MIN_VALUE);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(-2))).isEqualTo(-2L);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(-1))).isEqualTo(-1L);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(0))).isEqualTo(0L);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(1))).isEqualTo(1L);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(2))).isEqualTo(2L);
        assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);
        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextLong();
            assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt();
            assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt(1 << 20);
            assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt(1 << 10);
            assertThat(EncodingUtils.decodeVarLong(EncodingUtils.encodeVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }
    }

    @Test
    public void testVarOrder() {
        SortedMap<byte[], Long> map = new TreeMap<>(UnsignedBytes.lexicographicalComparator());

        while (map.size() < 1000) {
            long nextLong = rand.nextLong();
            if (nextLong >= 0) {
                byte[] encode = EncodingUtils.encodeVarLong(nextLong);
                map.put(encode, nextLong);
            }
        }

        while (map.size() < 2000) {
            long nextLong = rand.nextInt();
            if (nextLong >= 0) {
                byte[] encode = EncodingUtils.encodeVarLong(nextLong);
                map.put(encode, nextLong);
            }
        }

        while (map.size() < 3000) {
            long nextLong = rand.nextInt(1 << 20);
            if (nextLong >= 0) {
                byte[] encode = EncodingUtils.encodeVarLong(nextLong);
                map.put(encode, nextLong);
            }
        }

        map.put(EncodingUtils.encodeVarLong(0), 0L);
        map.put(EncodingUtils.encodeVarLong(1), 1L);
        map.put(EncodingUtils.encodeVarLong(2), 2L);
        map.put(EncodingUtils.encodeVarLong(Long.MAX_VALUE), Long.MAX_VALUE);

        long last = 0;
        for (Map.Entry<byte[], Long> e : map.entrySet()) {
            long l = e.getValue();
            assertThat(EncodingUtils.decodeVarLong(e.getKey())).isEqualTo(l);
            assertThat(e.getKey()).isEqualTo(EncodingUtils.encodeVarLong(l));
            if (l < last) {
                String lString = PtBytes.encodeHexString(EncodingUtils.encodeVarLong(l));
                String lastString = PtBytes.encodeHexString(EncodingUtils.encodeVarLong(last));
                throw new IllegalArgumentException("num: " + l + " last: " + last + " l:" + lString + " last: "
                        + lastString + " lastlen: " + lastString.length() / 2);
            }
            last = l;
        }
    }

    @Test
    public void testVarSigned() {
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(Long.MIN_VALUE)))
                .isEqualTo(Long.MIN_VALUE);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(-2)))
                .isEqualTo(-2L);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(-1)))
                .isEqualTo(-1L);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(0)))
                .isEqualTo(0L);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(1)))
                .isEqualTo(1L);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(2)))
                .isEqualTo(2L);
        assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);
        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextLong();
            assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt();
            assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt(1 << 20);
            assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }

        for (int i = 0; i < 1000; i++) {
            long nextLong = rand.nextInt(1 << 10);
            assertThat(EncodingUtils.decodeSignedVarLong(EncodingUtils.encodeSignedVarLong(nextLong)))
                    .isEqualTo(nextLong);
        }
    }

    @Test
    public void testMultipleVarSigned() {
        long[] longs = new long[1000];
        byte[][] bytes = new byte[1000][];
        for (int i = 0; i < 1000; i++) {
            longs[i] = rand.nextLong();
            bytes[i] = EncodingUtils.encodeSignedVarLong(longs[i]);
        }
        byte[] b = EncodingUtils.add(bytes);

        int index = 0;
        for (int i = 0; i < 1000; i++) {
            assertThat(EncodingUtils.decodeSignedVarLong(b, index)).isEqualTo(longs[i]);
            index += EncodingUtils.sizeOfSignedVarLong(longs[i]);
        }
    }

    @Test
    public void testVarSignedOrder() {
        SortedMap<byte[], Long> map = new TreeMap<>(UnsignedBytes.lexicographicalComparator());

        while (map.size() < 1000) {
            long nextLong = rand.nextLong();
            byte[] encode = EncodingUtils.encodeSignedVarLong(nextLong);
            map.put(encode, nextLong);
        }

        while (map.size() < 2000) {
            long nextLong = rand.nextInt();
            byte[] encode = EncodingUtils.encodeSignedVarLong(nextLong);
            map.put(encode, nextLong);
        }

        while (map.size() < 3000) {
            long nextLong = rand.nextInt(1 << 20);
            if (rand.nextBoolean()) {
                nextLong *= -1;
            }
            byte[] encode = EncodingUtils.encodeSignedVarLong(nextLong);
            map.put(encode, nextLong);
        }

        map.put(EncodingUtils.encodeSignedVarLong(Long.MIN_VALUE), Long.MIN_VALUE);
        map.put(EncodingUtils.encodeSignedVarLong(-2), -2L);
        map.put(EncodingUtils.encodeSignedVarLong(-1), -1L);
        map.put(EncodingUtils.encodeSignedVarLong(0), 0L);
        map.put(EncodingUtils.encodeSignedVarLong(1), 1L);
        map.put(EncodingUtils.encodeSignedVarLong(2), 2L);
        map.put(EncodingUtils.encodeSignedVarLong(Long.MAX_VALUE), Long.MAX_VALUE);

        long last = Long.MIN_VALUE;
        for (Map.Entry<byte[], Long> e : map.entrySet()) {
            long l = e.getValue();
            assertThat(EncodingUtils.decodeSignedVarLong(e.getKey())).isEqualTo(l);
            assertThat(e.getKey()).isEqualTo(EncodingUtils.encodeSignedVarLong(l));
            if (l < last) {
                String lString = PtBytes.encodeHexString(EncodingUtils.encodeVarLong(l));
                String lastString = PtBytes.encodeHexString(EncodingUtils.encodeVarLong(last));
                throw new IllegalArgumentException("num: " + l + " last: " + last + " num:" + lString + " last: "
                        + lastString + " lastlen: " + lastString.length() / 2);
            }
            last = l;
        }
    }

    @Test
    public void testMulti() {
        List<ValueType> valueTypes = ImmutableList.of(
                ValueType.FIXED_LONG,
                ValueType.FIXED_LONG_LITTLE_ENDIAN,
                ValueType.SHA256HASH,
                ValueType.VAR_LONG,
                ValueType.VAR_SIGNED_LONG,
                ValueType.VAR_STRING);

        Random random = new Random();
        for (int j = 0; j < 50; j++) {
            byte[] bytesToHash = new byte[256];
            rand.nextBytes(bytesToHash);
            List<Object> defaultComponents = ImmutableList.<Object>of(
                    rand.nextLong(),
                    rand.nextLong(),
                    Sha256Hash.computeHash(bytesToHash),
                    rand.nextLong() & Long.MAX_VALUE,
                    rand.nextLong(),
                    new BigInteger(100, random).toString(32));

            List<EncodingType> types = new ArrayList<>();
            List<Object> components = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                int index = rand.nextInt(valueTypes.size());
                ValueType type = valueTypes.get(index);
                ValueByteOrder order = rand.nextBoolean() ? ValueByteOrder.ASCENDING : ValueByteOrder.DESCENDING;
                types.add(new EncodingType(type, order));
                components.add(defaultComponents.get(index));
            }

            byte[] b = EncodingUtils.toBytes(types, components);

            List<Object> result = EncodingUtils.fromBytes(b, types);

            assertThat(result).isEqualTo(components);
        }
    }
}
