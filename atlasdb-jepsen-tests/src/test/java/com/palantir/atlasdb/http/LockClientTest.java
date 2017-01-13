/**
 * Copyright 2017 Palantir Technologies
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
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.Collections;

import org.junit.Test;

import com.palantir.lock.BlockingMode;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;

public class LockClientTest {
    private static final RemoteLockService LOCK_SERVICE = mock(RemoteLockService.class);
    private static final LockRefreshToken TOKEN = new LockRefreshToken(BigInteger.ONE, 1L);
    private static final String CLIENT = "client";
    private static final String LOCK_NAME = "lock";

    @Test
    public void lockRequestIsNonBlocking() throws InterruptedException {
        when(LockClient.lock(LOCK_SERVICE, CLIENT, LOCK_NAME)).thenAnswer((invocation) -> {
            LockRequest request = (LockRequest) invocation.getArguments()[1];
            assertThat(request.getBlockingMode(), is(BlockingMode.DO_NOT_BLOCK));
            return null;
        });
    }

    @Test
    public void lockRequestIsWrite() throws InterruptedException {
        when(LockClient.lock(LOCK_SERVICE, CLIENT, LOCK_NAME)).thenAnswer((invocation) -> {
            LockRequest request = (LockRequest) invocation.getArguments()[1];
            assertThat(request.getLocks(), contains(
                    hasProperty("lockMode", is(LockMode.WRITE))));
            return null;
        });
    }

    @Test
    public void unlockReturnsFalseIfTokenIsNull() throws InterruptedException {
        assertFalse(LockClient.unlock(LOCK_SERVICE, null));
    }

    @Test
    public void refreshReturnsNullIfTokenIsNull() {
        assertNull(LockClient.refresh(LOCK_SERVICE, null));
    }

    @Test
    public void refreshReturnsNullIfThereAreNoRefreshTokens() {
        when(LOCK_SERVICE.refreshLockRefreshTokens(any())).thenReturn(Collections.emptySet());
        assertNull(LockClient.refresh(LOCK_SERVICE, TOKEN));
    }
}
