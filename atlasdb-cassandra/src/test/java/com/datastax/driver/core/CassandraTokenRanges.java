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

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedTokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedTokenRange;
import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;

// Utility class - it's in com.datastax.driver.core package in order to allow us to create Tokens and TokenRanges
// on the fly for unit testing purposes.
public final class CassandraTokenRanges {
    private CassandraTokenRanges() {
        // utility class
    }

    public static TokenRange create(String startHexBinary, String endHexBinary) {
        return create(getToken(startHexBinary), getToken(endHexBinary));
    }

    public static TokenRange create(ByteOrderedToken start, ByteOrderedToken end) {
        return new ByteOrderedTokenRange(start, end);
    }

    public static ByteOrderedToken getToken(String hexBinary) {
        return new ByteOrderedToken(ByteBuffer.wrap(DatatypeConverter.parseHexBinary(hexBinary)));
    }

    public static ByteOrderedToken getToken(ByteBuffer byteBuffer) {
        return new ByteOrderedToken(byteBuffer);
    }

    public static ByteOrderedToken minToken() {
        // The minimum token is a special value that no key ever hashes to.
        // It's used both as lower and upper bound.
        return ByteOrderedTokenFactory.MIN_TOKEN;
    }
}
