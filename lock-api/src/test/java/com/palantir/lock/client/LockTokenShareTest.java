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

import com.palantir.lock.v2.LockToken;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class LockTokenShareTest {
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());

    @Test
    public void allSharedLocksHaveCorrectUnderlyingToken() {
        List<LockTokenShare> tokens = getSharedLockTokens(LOCK_TOKEN, 3);
        assertThat(tokens).extracting(LockTokenShare::sharedLockToken).containsOnly(LOCK_TOKEN);
    }

    @Test
    public void allSharedTokensShouldHaveDifferentIds() {
        Stream<LockToken> tokens = LockTokenShare.share(LOCK_TOKEN, 3);
        assertThat(tokens)
                .extracting(LockToken::getRequestId)
                .doesNotHaveDuplicates()
                .hasSize(3);
    }

    @Test
    public void shouldReturnUnderlyingTokenAfterAllReferencesAreUnlocked() {
        List<LockTokenShare> tokens = getSharedLockTokens(LOCK_TOKEN, 2);
        LockTokenShare firstToken = tokens.get(0);
        LockTokenShare secondToken = tokens.get(1);

        assertThat(firstToken.unlock())
                .as("no token as second token still has reference")
                .isEmpty();
        assertThat(firstToken.unlock())
                .as("no token as second token still has reference")
                .isEmpty();
        assertThat(secondToken.unlock())
                .as("get lock token since both references are unlocked")
                .contains(LOCK_TOKEN);
        assertThat(firstToken.unlock())
                .as("returns empty as unlock should return only once")
                .isEmpty();
    }

    @Test
    public void shouldWorkAsExpectedIfThereIsOnlyOneSharedToken() {
        List<LockTokenShare> tokens = getSharedLockTokens(LOCK_TOKEN, 1);

        assertThat(tokens).hasSize(1);
        LockTokenShare lockTokenShare = tokens.get(0);

        assertThat(lockTokenShare.sharedLockToken()).isEqualTo(LOCK_TOKEN);
        assertThat(lockTokenShare.getRequestId()).isNotEqualTo(LOCK_TOKEN.getRequestId());
        assertThat(lockTokenShare.unlock()).contains(LOCK_TOKEN);
    }

    @Test
    public void shouldThrowIfCreatedWithLessThanOneReference() {
        assertThatThrownBy(() -> LockTokenShare.share(LOCK_TOKEN, 0))
                .hasMessage("Reference count should be more than zero");
    }

    @Test
    public void sharingSharedLockTokenThrows() {
        LockTokenShare lockTokenShare = getSharedLockTokens(LOCK_TOKEN, 2).get(0);
        assertThatThrownBy(() -> LockTokenShare.share(lockTokenShare, 2))
                .hasMessage("Can not share a shared lock token");
    }

    private static List<LockTokenShare> getSharedLockTokens(LockToken token, int numberOfReferences) {
        return LockTokenShare.share(LOCK_TOKEN, numberOfReferences)
                .map(LockTokenShare.class::cast)
                .collect(Collectors.toList());
    }
}
