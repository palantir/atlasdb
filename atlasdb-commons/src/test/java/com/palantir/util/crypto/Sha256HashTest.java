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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
                .isEqualTo(expectedSha256(inputBytes))
                .isEqualTo(legacyHex(sha256Bytes));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "Hello, World", "12345678901234567890"})
    public void testComputeHash(String input) {
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        String expectedSha256 = expectedSha256(inputBytes);

        Sha256Hash hash = Sha256Hash.computeHash(inputBytes);
        String legacyHexEncoded = legacyHex(
                Sha256Hash.LOWER_CASE_HEX.decode(hash.serializeToHexString().toLowerCase(Locale.ROOT)));
        assertThat(hash)
                .isEqualTo(Sha256Hash.computeHash(inputBytes))
                .hasSameHashCodeAs(Sha256Hash.computeHash(inputBytes))
                .isEqualByComparingTo(Sha256Hash.computeHash(inputBytes))
                .asString()
                .hasSize(64)
                .matches("[0-9a-f]{64}")
                .isEqualTo(expectedSha256)
                .isEqualTo(hash.serializeToHexString())
                .isEqualTo(legacyHexEncoded);
        assertThat(hash.toString()).hasSize(64).isEqualTo(legacyHexEncoded);
    }

    private static String expectedSha256(byte[] bytes) {
        try {
            byte[] sha256 = Sha256Hash.getMessageDigest().digest(bytes);
            assertThat(MessageDigest.getInstance("SHA-256").digest(bytes)).isEqualTo(sha256);
            return Sha256Hash.LOWER_CASE_HEX.encode(sha256);
        } catch (NoSuchAlgorithmException e) {
            fail(e);
            throw new AssertionError(e);
        }
    }

    private static String legacyHex(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            s.append(String.format("%02x", bytes[i])); // $NON-NLS-1$
        }
        return s.toString();
    }
}
