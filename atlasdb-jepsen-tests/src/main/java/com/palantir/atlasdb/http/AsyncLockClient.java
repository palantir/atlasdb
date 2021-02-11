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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.NamespacedConjureTimelockServiceImpl;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.logsafe.Preconditions;
import java.util.List;
import java.util.Set;

public final class AsyncLockClient implements JepsenLockClient<LockToken> {
    private static final String NAMESPACE = "test";
    private final TimelockService timelockService;

    private AsyncLockClient(
            NamespacedTimelockRpcClient timelockService, NamespacedConjureTimelockService conjureTimelockService) {
        this.timelockService = RemoteTimelockServiceAdapter.create(
                timelockService, conjureTimelockService, NoOpLockWatchEventCache.create());
    }

    public static AsyncLockClient create(MetricsManager metricsManager, List<String> hosts) {
        return new AsyncLockClient(
                new NamespacedTimelockRpcClient(
                        TimelockUtils.createClient(metricsManager, hosts, TimelockRpcClient.class), NAMESPACE),
                new NamespacedConjureTimelockServiceImpl(
                        TimelockUtils.createClient(metricsManager, hosts, ConjureTimelockService.class), NAMESPACE));
    }

    @Override
    public LockToken lock(String client, String lockName) {
        LockRequest lockRequest =
                LockRequest.of(ImmutableSet.of(StringLockDescriptor.of(lockName)), Integer.MAX_VALUE, client);
        LockResponse lockResponse = timelockService.lock(lockRequest);
        Preconditions.checkState(
                lockResponse.wasSuccessful(),
                "Jepsen failed to lock a lock, but it would wait for Integer.MAX_VALUE, so this is unexpected.");
        return lockResponse.getToken();
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> lockTokenV2s) {
        return timelockService.unlock(lockTokenV2s);
    }

    @Override
    public Set<LockToken> refresh(Set<LockToken> lockTokenV2s) {
        return timelockService.refreshLockLeases(lockTokenV2s);
    }
}
