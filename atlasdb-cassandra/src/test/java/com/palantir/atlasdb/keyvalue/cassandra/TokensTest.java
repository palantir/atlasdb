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
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public final class TokensTest {
    @Test
    public void decodeHex() {
        generateTokens().forEach(input -> assertThat(Tokens.hexDecode(input))
                .as("token '%s'", input)
                .hasSize(input.length() / 2));
    }

    @Test
    public void isAllLowercaseOrDigits() {
        assertThat(Tokens.isAllLowercaseOrDigits("abcdef01234567890")).isTrue();
        assertThat(Tokens.isAllLowercaseOrDigits("01234567890abcdef")).isTrue();
        assertThat(Tokens.isAllLowercaseOrDigits("Abcdef01234567890")).isFalse();
        assertThat(Tokens.isAllLowercaseOrDigits("01234567890ABCDEF")).isFalse();
    }

    @Test
    public void allMatch() {
        assertThat(Tokens.allMatch("01234567890", Character::isDigit)).isTrue();
        assertThat(Tokens.allMatch("0123456789A", Character::isDigit)).isFalse();
        assertThat(Tokens.allMatch("ABCEFGHIJKLMNOPQRSTUVWXYZ", Character::isUpperCase))
                .isTrue();
        assertThat(Tokens.allMatch("ABCEFGHIJKLMNOPQRSTUVWXYZ", Character::isLowerCase))
                .isFalse();
        assertThat(Tokens.allMatch("abcefghijklmnopqrstuvwxyz", Character::isLowerCase))
                .isTrue();
        assertThat(Tokens.allMatch("abcefghijklmnopqrstuvwxyz", Character::isUpperCase))
                .isFalse();
    }

    static Stream<String> generateTokens() {
        return Stream.concat(
                        Stream.of(
                                "",
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
                                IntStream.range(0, 1_000).mapToObj(_i -> BaseEncoding.base16()
                                        .encode(Longs.toByteArray(
                                                ThreadLocalRandom.current().nextLong())))))
                .flatMap(input -> Stream.of(
                        input, input.toLowerCase(Locale.ROOT), input.toUpperCase(Locale.ROOT), shuffleCase(input)));
    }

    private static String shuffleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (Character.isAlphabetic(ch) && ThreadLocalRandom.current().nextBoolean()) {
                if (Character.isLowerCase(ch)) {
                    ch = Character.toUpperCase(ch);
                } else {
                    ch = Character.toLowerCase(ch);
                }
            }
            sb.append(ch);
        }
        return sb.toString();
    }
}
