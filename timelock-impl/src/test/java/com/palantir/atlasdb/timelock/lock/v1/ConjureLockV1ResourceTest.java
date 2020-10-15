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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.LockRefreshToken;
import java.math.BigInteger;
import org.junit.Test;

public class ConjureLockV1ResourceTest {
    private static final BigInteger TOKEN_ID = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
    private static final long EXPIRATION_DATE_MS = 123456789L;
    private static final LockRefreshToken LEGACY_TOKEN = new LockRefreshToken(TOKEN_ID, EXPIRATION_DATE_MS);
    private static final ConjureLockRefreshToken CONJURE_TOKEN = ConjureLockRefreshToken.of(TOKEN_ID,
            EXPIRATION_DATE_MS);

    @Test
    public void translationOfTokensPreservesIdAndExpiration() {
        assertThat(ConjureLockV1Tokens.getConjureTokens(ImmutableSet.of(LEGACY_TOKEN)))
                .containsExactly(CONJURE_TOKEN);
        assertThat(ConjureLockV1Tokens.getLegacyTokens(ImmutableList.of(CONJURE_TOKEN)))
                .containsExactly(LEGACY_TOKEN);
    }

    @Test
    public void multipleTokensAreTranslatedInSequence() {
        long expirationDateMs = 585L;
        ConjureLockRefreshToken otherToken = ConjureLockRefreshToken.of(BigInteger.ZERO, expirationDateMs);
        assertThat(ConjureLockV1Tokens.getLegacyTokens(ImmutableList.of(CONJURE_TOKEN, otherToken)))
                .containsExactly(LEGACY_TOKEN, new LockRefreshToken(BigInteger.ZERO, expirationDateMs));
    }
}
