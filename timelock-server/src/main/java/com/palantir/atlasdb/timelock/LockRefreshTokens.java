/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock;

import java.math.BigInteger;
import java.util.UUID;

import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.v2.LockTokenV2;

public class LockRefreshTokens {

    public static LockRefreshToken fromLockTokenV2(LockTokenV2 token) {
        String asHex = token.getRequestId().toString().replaceAll("-", "");
        BigInteger bigInt = new BigInteger(asHex, 16);
        return new LockRefreshToken(bigInt, System.currentTimeMillis() + 20_000);
    }

    public static LockTokenV2 toLockTokenV2(LockRefreshToken token) {
        long msb = token.getTokenId().shiftRight(64).longValue();
        long lsb = token.getTokenId().longValue();
        UUID requestId = new UUID(msb, lsb);
        return LockTokenV2.of(requestId);
    }

}
