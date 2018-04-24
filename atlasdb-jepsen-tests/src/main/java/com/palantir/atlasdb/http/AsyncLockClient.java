/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.util.List;
import java.util.Set;

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

    public static AsyncLockClient create(List<String> hosts) {
        return new AsyncLockClient(TimelockUtils.createClient(hosts, TimelockService.class));
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
