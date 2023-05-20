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

final class Tokens {

    private static final BaseEncoding LOWER_CASE_HEX = BaseEncoding.base16().lowerCase();

    static byte[] hexDecode(String token) {
        // OPP tokens should be lowercase already, use upper if needed, and convert mixed to uppercase if needed
        byte[] decoded;
        if (Tokens.isAllLowercaseOrDigits(token)) {
            decoded = LOWER_CASE_HEX.decode(token);
        } else {
            decoded = BaseEncoding.base16().decode(token.toUpperCase(Locale.ROOT));
        }
        return decoded;
    }

    @VisibleForTesting
    static boolean isAllLowercaseOrDigits(String token) {
        return allMatch(token, ch -> Character.isLowerCase(ch) || Character.isDigit(ch));
    }

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
