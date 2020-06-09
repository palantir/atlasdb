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

package com.palantir.atlasdb.timelock.lock.v1;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.LockRefreshToken;

final class ConjureLockV1Tokens {
    private ConjureLockV1Tokens() {
        // no
    }

    static List<LockRefreshToken> getLegacyTokens(List<ConjureLockRefreshToken> request) {
        return request.stream()
                .map(token -> new LockRefreshToken(token.getTokenId(), token.getExpirationDateMs()))
                .collect(Collectors.toList());
    }

    static Set<ConjureLockRefreshToken> getConjureTokens(Set<LockRefreshToken> serverTokens) {
        return serverTokens.stream()
                .map(token -> ConjureLockRefreshToken.of(token.getTokenId(), token.getExpirationDateMs()))
                .collect(Collectors.toSet());
    }
}
