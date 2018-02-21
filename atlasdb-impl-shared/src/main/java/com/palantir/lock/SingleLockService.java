/*
 * Copyright 2018 Palantir Technologies
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
package com.palantir.lock;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;

public class SingleLockService implements AutoCloseable {
    private final RemoteLockService lockService;
    private final String lockId;

    private LockRefreshToken token = null;

    public SingleLockService(RemoteLockService lockService, String lockId) {
        this.lockService = lockService;
        this.lockId = lockId;
    }

    public void lockOrRefresh() throws InterruptedException {
        if (token != null) {
            Set<LockRefreshToken> refreshedTokens = lockService.refreshLockRefreshTokens(ImmutableList.of(token));
            if (refreshedTokens.isEmpty()) {
                token = null;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of(lockId);
            LockRequest request = LockRequest.builder(
                    ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
            token = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        }
    }

    public boolean haveLocks() {
        return token != null;
    }

    @Override
    public void close() {
        if (token != null) {
            lockService.unlock(token);
        }
    }
}
