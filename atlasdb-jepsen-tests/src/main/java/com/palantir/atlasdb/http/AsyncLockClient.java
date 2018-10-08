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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;

public final class AsyncLockClient implements JepsenLockClient<LockToken> {
    private final TimelockService timelockService;

    private AsyncLockClient(TimelockService timelockService) {
        this.timelockService = timelockService;
    }

    public static AsyncLockClient create(MetricRegistry metricRegistry, List<String> hosts) {
        return new AsyncLockClient(TimelockUtils.createClient(metricRegistry, hosts, TimelockService.class));
    }

    @Override
    public LockToken lock(String client, String lockName) throws InterruptedException {
        LockRequest lockRequest = LockRequest.of(
                ImmutableSet.of(StringLockDescriptor.of(lockName)),
                Long.MAX_VALUE,
                client);
        LockResponse lockResponse = timelockService.lock(lockRequest);
        Preconditions.checkState(lockResponse.wasSuccessful(),
                "Jepsen failed to lock a lock, but it would wait for Long.MAX_VALUE, so this is unexpected.");
        return lockResponse.getToken();
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> lockTokenV2s) throws InterruptedException {
        return timelockService.unlock(lockTokenV2s);
    }

    @Override
    public Set<LockToken> refresh(Set<LockToken> lockTokenV2s) throws InterruptedException {
        return timelockService.refreshLockLeases(lockTokenV2s);
    }
}
