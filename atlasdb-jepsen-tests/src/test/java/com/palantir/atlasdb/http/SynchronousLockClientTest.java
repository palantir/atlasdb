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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.BlockingMode;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import java.math.BigInteger;
import java.util.Collections;
import org.junit.Test;

public class SynchronousLockClientTest {
    private static final LockService LOCK_SERVICE = mock(LockService.class);
    private static final SynchronousLockClient LOCK_CLIENT = new SynchronousLockClient(LOCK_SERVICE);
    private static final LockRefreshToken TOKEN_1 = new LockRefreshToken(BigInteger.ONE, 1L);
    private static final LockRefreshToken TOKEN_2 = new LockRefreshToken(BigInteger.TEN, 10L);

    private static final String CLIENT = "client";
    private static final String LOCK_NAME = "lock";

    @Test
    public void lockRequestIsNonBlocking() throws InterruptedException {
        when(LOCK_CLIENT.lock(CLIENT, LOCK_NAME)).thenAnswer(invocation -> {
            LockRequest request = invocation.getArgument(1);
            assertThat(request.getBlockingMode()).isEqualTo(BlockingMode.DO_NOT_BLOCK);
            return null;
        });
        LOCK_CLIENT.lock(CLIENT, LOCK_NAME);
    }

    @Test
    public void lockRequestIsWrite() throws InterruptedException {
        when(LOCK_CLIENT.lock(CLIENT, LOCK_NAME)).thenAnswer(invocation -> {
            LockRequest request = invocation.getArgument(1);
            assertThat(request.getLocks().stream()
                            .filter(lock -> lock.getLockMode().equals(LockMode.WRITE))
                            .findAny())
                    .isPresent();
            return null;
        });
        LOCK_CLIENT.lock(CLIENT, LOCK_NAME);
    }

    @Test
    public void unlockSingleReturnsFalseIfTokenIsNull() throws InterruptedException {
        assertThat(LOCK_CLIENT.unlockSingle(null)).isFalse();
    }

    @Test
    public void unlockSingleReturnsTrueIfTokenCanBeUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(eq(TOKEN_1))).thenReturn(true);
        assertThat(LOCK_CLIENT.unlockSingle(TOKEN_1)).isTrue();
    }

    @Test
    public void unlockReturnsEmptySetIfTokensWereAlreadyUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(any(LockRefreshToken.class))).thenReturn(false);
        assertThat(LOCK_CLIENT.unlock(ImmutableSet.of(TOKEN_1, TOKEN_2))).isEmpty();
    }

    @Test
    public void unlockReturnsTokensThatWereUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(eq(TOKEN_1))).thenReturn(false);
        when(LOCK_SERVICE.unlock(eq(TOKEN_2))).thenReturn(true);
        assertThat(LOCK_CLIENT.unlock(ImmutableSet.of(TOKEN_1, TOKEN_2))).containsExactlyInAnyOrder(TOKEN_2);
    }

    @Test
    public void refreshSingleReturnsNullIfTokenIsNull() throws InterruptedException {
        assertThat(LOCK_CLIENT.refreshSingle(null)).isNull();
    }

    @Test
    public void refreshSingleReturnsNullIfThereAreNoRefreshTokens() throws InterruptedException {
        when(LOCK_SERVICE.refreshLockRefreshTokens(any())).thenReturn(Collections.emptySet());
        assertThat(LOCK_CLIENT.refreshSingle(TOKEN_1)).isNull();
    }
}
