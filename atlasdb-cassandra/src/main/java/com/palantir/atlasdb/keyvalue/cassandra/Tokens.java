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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.BaseEncoding;
import java.util.Locale;
import java.util.function.IntPredicate;

/**
 * Optimized utility methods for handling Cassandra Order Preserving Partition
 * tokens that avoid allocations and copies where possible.
 */
final class Tokens {

    private static final BaseEncoding LOWER_CASE_HEX = BaseEncoding.base16().lowerCase();

    /**
     * Decodes hex encoded token value.
     * Cassandra 2.x uses lower case encoded OPP tokens, so this implementation
     * assumes that the token is lowercase base16 encoded for the fast path,
     * but supports slower fallback to convert token to uppercase and decode.
     *
     * @param token hex encoded token value
     * @return decoded bytes
     */
    static byte[] hexDecode(String token) {
        // OPP tokens should be lowercase -- fast path assumes this; slow fallback converts to uppercase
        return Tokens.isAllLowercaseOrDigits(token)
                ? LOWER_CASE_HEX.decode(token)
                : BaseEncoding.base16().decode(token.toUpperCase(Locale.ROOT));
    }

    /**
     * @return true if all characters of token are lowercase or digits
     */
    @VisibleForTesting
    static boolean isAllLowercaseOrDigits(String token) {
        return allMatch(token, ch -> Character.isLowerCase(ch) || Character.isDigit(ch));
    }

    /**
     * @return true if all characters of token pass predicate test
     */
    @VisibleForTesting
    static boolean allMatch(String token, IntPredicate predicate) {
        for (int i = 0; i < token.length(); i++) {
            char ch = token.charAt(i);
            if (!predicate.test(ch)) {
                return false;
            }
        }
        return true;
    }

    private Tokens() {}
}
