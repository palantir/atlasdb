/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import javax.annotation.Nonnull;

import com.palantir.lock.AutoDelegate_LockService;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockService;

public class LockRequestSizeLimitingLockService implements AutoDelegate_LockService {
    private final LockService delegate;

    public LockRequestSizeLimitingLockService(LockService delegate) {
        this.delegate = delegate;
    }

    @Override
    public LockService delegate() {
        return delegate;
    }

    @Override
    public LockResponse lockWithFullLockResponse(@Nonnull LockClient client, @Nonnull LockRequest request)
            throws InterruptedException {
        LockRequestSizeUtils.validateLockRequestSize(request);
        return delegate().lockWithFullLockResponse(client, request);
    }

    @Override
    public LockRefreshToken lock(@Nonnull String client, @Nonnull LockRequest request) throws InterruptedException {
        LockRequestSizeUtils.validateLockRequestSize(request);
        return delegate().lock(client, request);
    }

    @Override
    public HeldLocksToken lockAndGetHeldLocks(@Nonnull String client, @Nonnull LockRequest request)
            throws InterruptedException {
        LockRequestSizeUtils.validateLockRequestSize(request);
        return delegate().lockAndGetHeldLocks(client, request);
    }
}
