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
package com.palantir.atlasdb.sweep;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;

class SweepLocks implements AutoCloseable {
    private final RemoteLockService lockService;

    private LockRefreshToken token = null;

    SweepLocks(RemoteLockService lockService) {
        this.lockService = lockService;
    }

    void lockOrRefresh() throws InterruptedException {
        if (token != null) {
            Set<LockRefreshToken> refreshedTokens = lockService.refreshLockRefreshTokens(ImmutableList.of(token));
            if (refreshedTokens.isEmpty()) {
                token = null;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of("atlas sweep");
            LockRequest request = LockRequest.builder(
                    ImmutableSortedMap.of(lock, LockMode.WRITE)).doNotBlock().build();
            token = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
        }
    }

    boolean haveLocks() {
        return token != null;
    }

    @Override
    public void close() {
        if (token != null) {
            lockService.unlock(token);
        }
    }
}
