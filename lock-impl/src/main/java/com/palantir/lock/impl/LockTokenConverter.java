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
package com.palantir.lock.impl;

import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.Preconditions;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public final class LockTokenConverter {

    private LockTokenConverter() {}

    static LockRefreshToken toLegacyToken(LockToken tokenV2) {
        return new LockRefreshToken(toBigInteger(tokenV2.getRequestId()), Long.MIN_VALUE);
    }

    public static LockToken toTokenV2(LockRefreshToken legacyToken) {
        return LockToken.of(toUuid(legacyToken.getTokenId()));
    }

    private static BigInteger toBigInteger(UUID uuid) {
        return new BigInteger(ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array());
    }

    private static UUID toUuid(BigInteger bigInteger) {
        Preconditions.checkArgument(bigInteger.bitLength() < 128, "Value has too many bits to be converted to a UUID");
        long msb = bigInteger.shiftRight(64).longValue();
        long lsb = bigInteger.longValue();
        return new UUID(msb, lsb);
    }
}
