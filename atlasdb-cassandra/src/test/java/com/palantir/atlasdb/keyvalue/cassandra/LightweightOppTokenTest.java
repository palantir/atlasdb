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

import com.palantir.conjure.java.lib.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class LightweightOppTokenTest {

    public static List<String> getParameters() {
        return TokensTest.generateTokens()
                .flatMap(input -> Stream.of(input, input.toLowerCase(Locale.ROOT), input.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

    @ParameterizedTest(name = "{index}: input=\"{0}\"")
    @MethodSource("getParameters")
    public void inputsAreValidBase16Strings(String input) {
        assertThat(input).matches("^[0-9a-fA-F]*$");
        assertThat(input.length() % 2)
                .as("input must be even length to base16 decode")
                .isEqualTo(0);
        assertThat(CassandraHex.hexToBytes(input)).hasSize(input.length() / 2);
    }

    @ParameterizedTest(name = "{index}: input=\"{0}\"")
    @MethodSource("getParameters")
    public void createFromHexEncodedStringRegardlessOfCase(String input) {
        LightweightOppToken token = LightweightOppToken.fromHex(input);
        LightweightOppToken tokenFromLowerCase = LightweightOppToken.fromHex(input.toLowerCase(Locale.ROOT));
        LightweightOppToken tokenFromUpperCase = LightweightOppToken.fromHex(input.toUpperCase(Locale.ROOT));
        LightweightOppToken tokenFromBytes = new LightweightOppToken(CassandraHex.hexToBytes(input));

        assertThat(token.bytes)
                .hasSize(input.length() / 2)
                .isEqualTo(tokenFromLowerCase.bytes)
                .isEqualTo(tokenFromUpperCase.bytes)
                .isEqualTo(tokenFromBytes.bytes);
        assertThat(token)
                .isNotNull()
                .isEqualTo(tokenFromLowerCase)
                .isEqualTo(tokenFromUpperCase)
                .isEqualTo(tokenFromBytes)
                .isEqualByComparingTo(tokenFromLowerCase)
                .isEqualByComparingTo(tokenFromUpperCase)
                .isEqualByComparingTo(tokenFromBytes);
    }

    @ParameterizedTest(name = "{index}: input=\"{0}\"")
    @MethodSource("getParameters")
    public void fromHexCorrectlyDeserializesStrings(String input) throws Exception {
        LightweightOppToken token = LightweightOppToken.fromHex(input);
        Bytes expectedBytes = Bytes.from(org.apache.commons.codec.binary.Hex.decodeHex(input));
        assertThat(token.bytes).contains(expectedBytes.asNewByteArray());
        assertThat(token.isEmpty()).isEqualTo(input.isEmpty());
        ByteBuffer deserialized = token.deserialize();
        assertThat(deserialized).isEqualTo(expectedBytes.asReadOnlyByteBuffer());
        assertThat(deserialized.remaining()).isEqualTo(input.length() / 2);
        assertThat(token.toString()).isEqualToIgnoringCase(input);
    }

    @ParameterizedTest(name = "{index}: input=\"{0}\"")
    @MethodSource("getParameters")
    public void tokenMatchesCassandraImplementation(String input) throws Exception {
        // Test compatibility with Cassandra hex conversions from:
        // https://github.com/palantir/cassandra/blob/palantir-cassandra-2.2.18/src/java/org/apache/cassandra/dht/ByteOrderedPartitioner.java#L74
        Bytes expectedBytes = Bytes.from(org.apache.commons.codec.binary.Hex.decodeHex(input));
        String hexFromCassandra = CassandraHex.bytesToHex(expectedBytes.asNewByteArray());
        byte[] bytesFromCassandra = CassandraHex.hexToBytes(hexFromCassandra);
        LightweightOppToken token = LightweightOppToken.fromHex(hexFromCassandra);
        assertThat(Tokens.isAllLowercaseOrDigits(hexFromCassandra))
                .as("Expect lowercase hex %s", hexFromCassandra)
                .isTrue();
        assertThat(token).isEqualTo(new LightweightOppToken(expectedBytes.asNewByteArray()));
        assertThat(token.bytes).contains(bytesFromCassandra);
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
    static final class CassandraHex {
        private static final byte[] charToByte = new byte[256];

        // package protected for use by ByteBufferUtil. Do not modify this array !!
        static final char[] byteToChar = new char[16];

        static {
            for (char c = 0; c < charToByte.length; ++c) {
                if (c >= '0' && c <= '9') {
                    charToByte[c] = (byte) (c - '0');
                } else if (c >= 'A' && c <= 'F') {
                    charToByte[c] = (byte) (c - 'A' + 10);
                } else if (c >= 'a' && c <= 'f') {
                    charToByte[c] = (byte) (c - 'a' + 10);
                } else {
                    charToByte[c] = (byte) -1;
                }
            }

            for (int i = 0; i < 16; ++i) {
                byteToChar[i] = Integer.toHexString(i).charAt(0);
            }
        }

        public static byte[] hexToBytes(String str) {
            if (str.length() % 2 == 1) {
                throw new NumberFormatException("An hex string representing bytes must have an even length");
            }

            byte[] bytes = new byte[str.length() / 2];
            for (int i = 0; i < bytes.length; i++) {
                byte halfByte1 = charToByte[str.charAt(i * 2)];
                byte halfByte2 = charToByte[str.charAt(i * 2 + 1)];
                if (halfByte1 == -1 || halfByte2 == -1) {
                    throw new NumberFormatException("Non-hex characters in " + str);
                }
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
