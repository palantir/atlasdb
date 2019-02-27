/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import com.palantir.lock.v2.LockToken;

public class SharedLockTokenTest {
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    @Test
    public void allSharedLocksHaveCorrectUnderlyingToken() {
        List<SharedLockToken> tokens = getSharedLockTokens(LOCK_TOKEN, 3);
        assertThat(tokens)
                .extracting(SharedLockToken::referencedToken)
                .containsOnly(LOCK_TOKEN);
    }

    @Test
    public void allSharedTokensShouldHaveDifferentIds() {
        List<LockToken> tokens = SharedLockToken.share(LOCK_TOKEN, 3);
        assertThat(tokens)
                .extracting(LockToken::getRequestId)
                .doesNotHaveDuplicates()
                .hasSize(3);
    }

    @Test
    public void shouldReturnUnderlyingTokenAfterAllReferencesAreUnlocked() {
        List<SharedLockToken> tokens = getSharedLockTokens(LOCK_TOKEN, 2);
        SharedLockToken firstToken = tokens.get(0);
        SharedLockToken secondToken = tokens.get(1);

        assertThat(firstToken.unlock()).isEmpty();
        assertThat(secondToken.unlock()).contains(LOCK_TOKEN);
    }

    @Test
    public void callingUnlockOnSameTokenMultipleTimesShouldNotUnlockUnderlyingToken() {
        List<SharedLockToken> tokens = getSharedLockTokens(LOCK_TOKEN, 2);

        SharedLockToken firstToken = tokens.get(0);

        assertThat(firstToken.unlock()).isEmpty();
        assertThat(firstToken.unlock()).isEmpty();
    }

    @Test
    public void shouldWorkAsExpectedIfThereIsOnlyOneSharedToken() {
        List<SharedLockToken> tokens = getSharedLockTokens(LOCK_TOKEN, 1);

        assertThat(tokens).hasSize(1);
        SharedLockToken sharedLockToken = tokens.get(0);

        assertThat(sharedLockToken.referencedToken()).isEqualTo(LOCK_TOKEN);
        assertThat(sharedLockToken.getRequestId()).isNotEqualTo(LOCK_TOKEN.getRequestId());
        assertThat(sharedLockToken.unlock()).contains(LOCK_TOKEN);
    }

    @Test
    public void shouldThrowIfCreatedWithLessThanOneReference() {
        assertThatThrownBy(() -> SharedLockToken.share(LOCK_TOKEN, 0))
                .hasMessage("Reference count should be more than zero");
    }

    private static List<SharedLockToken> getSharedLockTokens(LockToken token, int numberOfReferences) {
        return SharedLockToken.share(LOCK_TOKEN, numberOfReferences).stream()
                .map(SharedLockToken.class::cast)
                .collect(Collectors.toList());
    }
}