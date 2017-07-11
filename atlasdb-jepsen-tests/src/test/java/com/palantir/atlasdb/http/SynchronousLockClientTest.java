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
package com.palantir.atlasdb.http;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.lock.BlockingMode;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public class SynchronousLockClientTest {
    private static final RemoteLockService LOCK_SERVICE = mock(RemoteLockService.class);
    private static final SynchronousLockClient LOCK_CLIENT = new SynchronousLockClient(LOCK_SERVICE);
    private static final LockRefreshToken TOKEN_1 = new LockRefreshToken(BigInteger.ONE, 1L);
    private static final LockRefreshToken TOKEN_2 = new LockRefreshToken(BigInteger.TEN, 10L);

    private static final String CLIENT = "client";
    private static final String LOCK_NAME = "lock";

    @Test
    public void lockRequestIsNonBlocking() throws InterruptedException {
        when(LOCK_CLIENT.lock(CLIENT, LOCK_NAME)).thenAnswer((invocation) -> {
            LockRequest request = (LockRequest) invocation.getArguments()[1];
            assertThat(request.getBlockingMode(), is(BlockingMode.DO_NOT_BLOCK));
            return null;
        });
        LOCK_CLIENT.lock(CLIENT, LOCK_NAME);
    }

    @Test
    public void lockRequestIsWrite() throws InterruptedException {
        when(LOCK_CLIENT.lock(CLIENT, LOCK_NAME)).thenAnswer((invocation) -> {
            LockRequest request = (LockRequest) invocation.getArguments()[1];
            assertThat(request.getLocks(), contains(hasProperty("lockMode", is(LockMode.WRITE))));
            return null;
        });
        LOCK_CLIENT.lock(CLIENT, LOCK_NAME);
    }

    @Test
    public void unlockSingleReturnsFalseIfTokenIsNull() throws InterruptedException {
        assertFalse(LOCK_CLIENT.unlockSingle(null));
    }

    @Test
    public void unlockSingleReturnsTrueIfTokenCanBeUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(eq(TOKEN_1))).thenReturn(true);
        assertTrue(LOCK_CLIENT.unlockSingle(TOKEN_1));
    }

    @Test
    public void unlockReturnsEmptySetIfTokensWereAlreadyUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(any())).thenReturn(false);
        assertThat(LOCK_CLIENT.unlock(ImmutableSet.of(TOKEN_1, TOKEN_2)), empty());
    }

    @Test
    public void unlockReturnsTokensThatWereUnlocked() throws InterruptedException {
        when(LOCK_SERVICE.unlock(eq(TOKEN_1))).thenReturn(false);
        when(LOCK_SERVICE.unlock(eq(TOKEN_2))).thenReturn(true);
        assertThat(LOCK_CLIENT.unlock(ImmutableSet.of(TOKEN_1, TOKEN_2)), containsInAnyOrder(TOKEN_2));
    }

    @Test
    public void refreshSingleReturnsNullIfTokenIsNull() throws InterruptedException {
        assertNull(LOCK_CLIENT.refreshSingle(null));
    }

    @Test
    public void refreshSingleReturnsNullIfThereAreNoRefreshTokens() throws InterruptedException {
        when(LOCK_SERVICE.refreshLockRefreshTokens(any())).thenReturn(Collections.emptySet());
        assertNull(LOCK_CLIENT.refreshSingle(TOKEN_1));
    }
}
