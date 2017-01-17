/**
 * Copyright 2016 Palantir Technologies
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;

public final class LockClient {
    private LockClient() {
    }

    public static RemoteLockService create(List<String> hosts) {
        return TimelockUtils.createClient(hosts, RemoteLockService.class);
    }

    public static LockRefreshToken lock(RemoteLockService service, String client, String lock)
            throws InterruptedException {
        LockDescriptor descriptor = StringLockDescriptor.of(lock);
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(descriptor, LockMode.WRITE))
                .doNotBlock()
                .build();
        return service.lock(client, request);
    }

    public static boolean unlock(RemoteLockService service, LockRefreshToken token) throws InterruptedException {
        if (token == null) {
            return false;
        }
        return service.unlock(token);
    }

    public static LockRefreshToken refresh(RemoteLockService service, LockRefreshToken token) {
        if (token == null) {
            return null;
        }
        Set<LockRefreshToken> validTokens = service.refreshLockRefreshTokens(ImmutableList.of(token));
        return Iterables.getOnlyElement(validTokens, null);
    }
}
