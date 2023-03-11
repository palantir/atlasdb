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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.palantir.conjure.java.lib.Bytes;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LightweightOppTokenTest {
    @Parameter()
    public String input;

    @Parameters(name = "{index}: input=\"{0}\"")
    public static Collection<Object[]> data() {
        return Stream.concat(
                        Stream.of(
                                "0F",
                                "decafbad",
                                "0123456789ABCDEF",
                                "0123456789abcdef",
                                "0123456789Abcdef",
                                "0123456789aBcdef",
                                "0123456789abCdef",
                                "0123456789abcDef",
                                "0123456789abcdEf",
                                "0123456789abcdeF",
                                Long.toHexString(Long.MIN_VALUE),
                                Long.toHexString(Long.MAX_VALUE)),
                        Stream.concat(
                                IntStream.range(0, 32).mapToObj(i -> {
                                    byte[] bytes = new byte[i * 2];
                                    ThreadLocalRandom.current().nextBytes(bytes);
                                    return BaseEncoding.base16().encode(bytes);
                                }),
                                IntStream.range(0, 1000).mapToObj(_i -> BaseEncoding.base16()
                                        .encode(Longs.toByteArray(
                                                ThreadLocalRandom.current().nextLong())))))
                .flatMap(input -> Stream.of(input, input.toLowerCase(Locale.ROOT), input.toUpperCase(Locale.ROOT)))
                .map(input -> new Object[] {input})
                .collect(Collectors.toList());
    }

    @Test
    public void createFromHex() throws Exception {
        assertThat(input.length() % 2 == 0)
                .as("input must be even length to base16 decode")
                .isTrue();
        int expectedByteLength = input.length() / 2;

        Bytes expectedBytes = Bytes.from(org.apache.commons.codec.binary.Hex.decodeHex(input));
        LightweightOppToken lowerToken = LightweightOppToken.fromHex(input.toLowerCase(Locale.ROOT));
        LightweightOppToken upperToken = LightweightOppToken.fromHex(input.toUpperCase(Locale.ROOT));
        LightweightOppToken token = LightweightOppToken.fromHex(input);
        LightweightOppToken token2 = new LightweightOppToken(Hex.hexToBytes(input));
        assertThat(token)
                .isNotNull()
                .isEqualTo(lowerToken)
                .isEqualTo(upperToken)
                .isEqualTo(token2)
                .isEqualByComparingTo(lowerToken)
                .isEqualByComparingTo(upperToken)
                .isEqualByComparingTo(token2);
        assertThat(token.bytes).hasSize(expectedByteLength).contains(expectedBytes.asNewByteArray());
        assertThat(token.isEmpty()).isEqualTo(input.length() == 0);
        assertThat(token.deserialize())
                .isEqualTo(expectedBytes.asReadOnlyByteBuffer())
                .asInstanceOf(InstanceOfAssertFactories.type(ByteBuffer.class))
                .extracting(Buffer::remaining)
                .asInstanceOf(InstanceOfAssertFactories.INTEGER)
                .isEqualTo(expectedByteLength);
        assertThat(token.toString()).isEqualToIgnoringCase(input);

        // Test compatibility with Cassandra hex conversions from:
        // https://github.com/palantir/cassandra/blob/palantir-cassandra-2.2.18/src/java/org/apache/cassandra/dht/ByteOrderedPartitioner.java#L74
        String hex = Hex.bytesToHex(expectedBytes.asNewByteArray());
        byte[] bytes = Hex.hexToBytes(hex);
        LightweightOppToken token4 = LightweightOppToken.fromHex(hex);
        assertThat(LightweightOppToken.isAllLowercaseOrDigits(hex))
                .as("Expect lowercase hex %s", hex)
                .isTrue();
        assertThat(token4).isEqualTo(new LightweightOppToken(expectedBytes.asNewByteArray()));
        assertThat(token4.bytes).contains(bytes);
    }

    /*
     * Licensed to the Apache Software Foundation (ASF) under one
     * or more contributor license agreements.  See the NOTICE file
     * distributed with this work for additional information
     * regarding copyright ownership.  The ASF licenses this file
     * to you under the Apache License, Version 2.0 (the
     * "License"); you may not use this file except in compliance
     * with the License.  You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    // Imported from cassandra to test using their conversions
    // https://github.com/palantir/cassandra/blob/palantir-cassandra-2.2.18/src/java/org/apache/cassandra/utils/Hex.java
    static final class Hex {
        private static final byte[] charToByte = new byte[256];

        // package protected for use by ByteBufferUtil. Do not modify this array !!
        static final char[] byteToChar = new char[16];

        static {
            for (char c = 0; c < charToByte.length; ++c) {
                if (c >= '0' && c <= '9') charToByte[c] = (byte) (c - '0');
                else if (c >= 'A' && c <= 'F') charToByte[c] = (byte) (c - 'A' + 10);
                else if (c >= 'a' && c <= 'f') charToByte[c] = (byte) (c - 'a' + 10);
                else charToByte[c] = (byte) -1;
            }

            for (int i = 0; i < 16; ++i) {
                byteToChar[i] = Integer.toHexString(i).charAt(0);
            }
        }

        public static byte[] hexToBytes(String str) {
            if (str.length() % 2 == 1)
                throw new NumberFormatException("An hex string representing bytes must have an even length");

            byte[] bytes = new byte[str.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                byte halfByte1 = charToByte[str.charAt(i * 2)];
                byte halfByte2 = charToByte[str.charAt(i * 2 + 1)];
                if (halfByte1 == -1 || halfByte2 == -1) throw new NumberFormatException("Non-hex characters in " + str);
                bytes[i] = (byte) ((halfByte1 << 4) | halfByte2);
            }
            return bytes;
        }

        public static String bytesToHex(byte... bytes) {
            char[] c = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                int bint = bytes[i];
                c[i * 2] = byteToChar[(bint & 0xf0) >> 4];
                c[1 + i * 2] = byteToChar[bint & 0x0f];
            }

            return new String(c);
        }
    }
}
