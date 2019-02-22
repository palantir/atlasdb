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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockRpcClient;

public final class AsyncLockClient implements JepsenLockClient<LockToken> {
    private static final LockResponseV2.Visitor<LockToken> LOCK_TOKEN_EXTRACTOR =
            LockResponseV2.Visitor.of(
                    LockResponseV2.Successful::getToken,
                    unsuccessful -> {
                        throw new IllegalStateException("Jepsen failed to lock a lock, but it would wait for "
                                + "Long.MAX_VALUE, so this is unexpected.");
                    });
    private final TimelockRpcClient timelockService;

    private AsyncLockClient(TimelockRpcClient timelockService) {
        this.timelockService = timelockService;
    }

    public static AsyncLockClient create(MetricRegistry metricRegistry, List<String> hosts) {
        return new AsyncLockClient(TimelockUtils.createClient(metricRegistry, hosts, TimelockRpcClient.class));
    }

    @Override
    public LockToken lock(String client, String lockName) throws InterruptedException {
        LockRequest lockRequest = LockRequest.of(
                ImmutableSet.of(StringLockDescriptor.of(lockName)),
                Long.MAX_VALUE,
                client);
        LockResponseV2 lockResponse = timelockService.lock(lockRequest);
        return lockResponse.accept(LOCK_TOKEN_EXTRACTOR);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> lockTokenV2s) throws InterruptedException {
        return timelockService.unlock(lockTokenV2s);
    }

    @Override
    public Set<LockToken> refresh(Set<LockToken> lockTokenV2s) throws InterruptedException {
        return timelockService.refreshLockLeases(lockTokenV2s).refreshedTokens();
    }
}
