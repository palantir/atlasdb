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

import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import com.palantir.lock.StringLockDescriptor;

public class SynchronousLockClient implements JepsenLockClient<LockRefreshToken> {
    private final LockService lockService;

    @VisibleForTesting
    SynchronousLockClient(LockService lockService) {
        this.lockService = lockService;
    }

    public static JepsenLockClient<LockRefreshToken> create(MetricsManager metricsManager, List<String> hosts) {
        return new SynchronousLockClient(TimelockUtils.createClient(metricsManager, hosts, LockService.class));
    }

    @Override
    public LockRefreshToken lock(String client, String lockName) throws InterruptedException {
        LockDescriptor descriptor = StringLockDescriptor.of(lockName);
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(descriptor, LockMode.WRITE))
                .doNotBlock()
                .build();

        return lockService.lock(client, request);
    }

    @Override
    public Set<LockRefreshToken> unlock(Set<LockRefreshToken> lockRefreshTokens) throws InterruptedException {
        Set<LockRefreshToken> tokensUnlocked = Sets.newHashSet();
        lockRefreshTokens.forEach(token -> {
            if (lockService.unlock(token)) {
                tokensUnlocked.add(token);
            }
        });
        return tokensUnlocked;
    }

    @Override
    public Set<LockRefreshToken> refresh(Set<LockRefreshToken> lockRefreshTokens) throws InterruptedException {
        return lockService.refreshLockRefreshTokens(lockRefreshTokens);
    }
}
