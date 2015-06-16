// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.lock.client;

import com.palantir.lock.BlockingMode;
import com.palantir.lock.ForwardingLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockGroupBehavior;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;

/**
 * This class splits its calls to two clients (which should be the same endpoint) based
 * on whether the call is blocking or non-blocking. This prevents blocking calls to
 * lock from starving out connections for calls like refresh or unlock which should
 * always complete quickly.
 */
public class ClientSplitLockService extends ForwardingLockService {

    private final LockService blockingClient;
    private final LockService nonBlockingClient;

    public ClientSplitLockService(LockService blockingClient, LockService nonBlockinClient) {
        this.blockingClient = blockingClient;
        this.nonBlockingClient = nonBlockinClient;
    }

    @Override
    protected LockService delegate() {
        return nonBlockingClient;
    }

    @Override
    public LockResponse lock(LockClient client, LockRequest request) throws InterruptedException {
        if (request.getBlockingMode() == BlockingMode.DO_NOT_BLOCK) {
            return delegate().lock(client, request);
        }

        // Let's try sending this request as a non-blocking request.
        if ((request.getLockGroupBehavior() == LockGroupBehavior.LOCK_ALL_OR_NONE)
                && (request.getBlockingMode() != BlockingMode.BLOCK_INDEFINITELY_THEN_RELEASE)) {
            LockRequest.Builder newRequest = LockRequest.builder(request.getLockDescriptors());
            newRequest.doNotBlock();
            newRequest.timeoutAfter(request.getLockTimeout());
            if (request.getVersionId() != null) {
                newRequest.withLockedInVersionId(request.getVersionId());
            }
            LockResponse response = nonBlockingClient.lock(client, newRequest.build());
            if (response.success()) {
                return response;
            }
        }

        // No choice but to send it as a blocking request.
        return blockingClient.lock(client, request);
    }
}
