/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.datastax.driver.core;

import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;

// Utility class - it's in com.datastax.driver.core package in order to allow us to create Tokens and TokenRanges
// on the fly for unit testing purposes.
public final class CassandraTokenRanges {
    private static final Token.Factory FACTORY = Token.OPPToken.FACTORY;

    private CassandraTokenRanges() {
        // utility class
    }

    public static TokenRange create(String startHexBinary, String endHexBinary) {
        Token startToken = getToken(startHexBinary);
        Token endToken = getToken(endHexBinary);
        return new TokenRange(startToken, endToken, FACTORY);
    }

    public static TokenRange create(Token start, Token end) {
        return new TokenRange(start, end, FACTORY);
    }

    public static Token getToken(String hexBinary) {
        return FACTORY.hash(ByteBuffer.wrap(DatatypeConverter.parseHexBinary(hexBinary)));
    }

    public static Token getToken(ByteBuffer byteBuffer) {
        return FACTORY.hash(byteBuffer);
    }

    public static Token maxToken() {
        // Not a typo; the minimum token is a special value that no key ever hashes to.
        // It's used both as lower and upper bound.
        return FACTORY.minToken();
    }
}
