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
package com.palantir.util.crypto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.base.Charsets;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class Sha256HashTest {

    @Test
    public void testSha256() {
        MessageDigest digest = Sha256Hash.getMessageDigest();
        assertThat(digest.getAlgorithm()).isEqualTo("SHA-256");
        assertThat(digest).isNotSameAs(Sha256Hash.getMessageDigest());
        byte[] inputBytes = "Hello World".getBytes(Charsets.UTF_8);
        byte[] sha256Bytes = digest.digest(inputBytes);
        assertThat(sha256Bytes).hasSize(32);
        assertThat(Sha256Hash.LOWER_CASE_HEX.encode(sha256Bytes))
                .hasSize(64)
                .isEqualToIgnoringCase("A591A6D40BF420404A011733CFB7B190D62C65BF0BCDA32B57B277D9AD9F146E")
                .isEqualTo(expectedSha256Hex(inputBytes))
                .isEqualTo(legacyHex(sha256Bytes));
    }

    static Stream<Arguments> params() {
        return Stream.concat(
                Stream.of(
                        Arguments.of("test", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                        Arguments.of(
                                "Hello, World", "03675ac53ff9cd1535ccc7dfcdfa2c458c5218371f418dc136f2d19ac1fbe8a5"),
                        Arguments.of(
                                "12345678901234567890",
                                "6ed645ef0e1abea1bf1e4e935ff04f9e18d39812387f63cda3415b46240f0405")),
                IntStream.range(1, 32).mapToObj(length -> {
                    StringBuilder sb = new StringBuilder(length);
                    for (int i = 0; i < length; i++) {
                        sb.append((char) ThreadLocalRandom.current().nextInt(32, 127));
                    }
                    String string = sb.toString();
                    return Arguments.of(string, expectedSha256Hex(string.getBytes(StandardCharsets.UTF_8)));
                }));
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testComputeHash(String input, String expectedToString) {
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        Sha256Hash hash = Sha256Hash.computeHash(inputBytes);

        assertThat(hash)
                .isEqualTo(Sha256Hash.computeHash(inputBytes))
                .hasSameHashCodeAs(Sha256Hash.computeHash(inputBytes))
                .isEqualByComparingTo(Sha256Hash.computeHash(inputBytes));

        assertThat(hash.getBytes()).isEqualTo(expectedSha256Bytes(inputBytes));

        assertThat(hash.serializeToHexString())
                .isEqualTo(legacyHex(hash.getBytes()))
                .satisfies(hex ->
                        assertThat(Sha256Hash.deSerializeFromHexString(hex)).isEqualTo(hash))
                .satisfies(hex -> assertThat(hash.getBytes())
                        .isEqualTo(Sha256Hash.LOWER_CASE_HEX.decode(hex.toLowerCase(Locale.ROOT)))
                        .isEqualTo(Sha256Hash.LOWER_CASE_HEX.decode(hex)));

        assertThat(hash)
                .asString()
                .hasSize(64)
                .matches("[0-9a-f]{64}")
                .isEqualTo(expectedToString)
                .isEqualTo(expectedSha256Hex(inputBytes))
                .isEqualTo(legacyHex(hash.getBytes()))
                .isEqualTo(hash.serializeToHexString());
    }

    @Test
    void testEmptyByteInput() {
        byte[] inputBytes = new byte[0];
        Sha256Hash hash = Sha256Hash.computeHash(inputBytes);

        assertThat(hash)
                .isEqualTo(Sha256Hash.computeHash(inputBytes))
                .hasSameHashCodeAs(Sha256Hash.computeHash(inputBytes))
                .isEqualByComparingTo(Sha256Hash.computeHash(inputBytes));

        assertThat(hash.getBytes()).isEqualTo(expectedSha256Bytes(inputBytes));
        assertThat(hash.serializeToHexString())
                .isEqualTo(legacyHex(hash.getBytes()))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        String legacyHexEncoded = legacyHex(hash.getBytes());
        assertThat(hash.serializeToHexString()).isEqualTo(legacyHexEncoded);
        assertThat(Sha256Hash.computeHash(inputBytes).toString()).isEqualTo("EMPTY");
    }

    private static String expectedSha256Hex(byte[] input) {
        byte[] sha256 = expectedSha256Bytes(input);
        String hexEncoded = Sha256Hash.LOWER_CASE_HEX.encode(sha256);
        assertThat(hexEncoded).isEqualTo(legacyHex(sha256));
        return hexEncoded;
    }

    private static byte[] expectedSha256Bytes(byte[] input) {
        try {
            byte[] sha256 = Sha256Hash.getMessageDigest().digest(input);
            assertThat(MessageDigest.getInstance("SHA-256").digest(input)).isEqualTo(sha256);
            return sha256;
        } catch (NoSuchAlgorithmException e) {
            fail(e);
            throw new AssertionError(e);
        }
    }

    /**
     * Verify backward compatibility with previous hex encoding
     */
    private static String legacyHex(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            s.append(String.format("%02x", bytes[i])); // $NON-NLS-1$
        }
        String legacyHex = s.toString();
        assertThat(Sha256Hash.LOWER_CASE_HEX.encode(bytes)).isEqualTo(legacyHex);
        return legacyHex;
    }
}
