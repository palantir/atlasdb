/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.ConjureSimpleHeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.SimpleHeldLocksToken;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class ConjureLockV1Tokens {
    private ConjureLockV1Tokens() {
        // no
    }

    public static List<LockRefreshToken> getLegacyTokens(Collection<ConjureLockRefreshToken> request) {
        return request.stream()
                .map(token -> new LockRefreshToken(token.getTokenId(), token.getExpirationDateMs()))
                .collect(Collectors.toList());
    }

    public static List<ConjureLockRefreshToken> getConjureTokens(Iterable<LockRefreshToken> serverTokens) {
        return StreamSupport.stream(serverTokens.spliterator(), false)
                .map(ConjureLockV1Tokens::getConjureToken)
                .collect(Collectors.toList());
    }

    public static ConjureLockRefreshToken getConjureToken(LockRefreshToken token) {
        return ConjureLockRefreshToken.of(token.getTokenId(), token.getExpirationDateMs());
    }

    public static ConjureSimpleHeldLocksToken getSimpleHeldLocksToken(SimpleHeldLocksToken legacyToken) {
        return ConjureSimpleHeldLocksToken.of(legacyToken.getTokenId(), legacyToken.getCreationDateMs());
    }
}
