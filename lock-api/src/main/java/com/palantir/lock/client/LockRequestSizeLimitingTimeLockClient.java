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

import com.palantir.lock.v2.AutoDelegate_TimelockRpcClient;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.TimelockRpcClient;

public class LockRequestSizeLimitingTimeLockClient implements AutoDelegate_TimelockRpcClient {
    private final TimelockRpcClient delegate;

    public LockRequestSizeLimitingTimeLockClient(TimelockRpcClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public TimelockRpcClient delegate() {
        return delegate;
    }

    @Override
    public LockResponse deprecatedLock(LockRequest request) {
        LockRequestSizeUtils.validateLockRequestSize(request);
        return delegate().deprecatedLock(request);
    }

    @Override
    public LockResponseV2 lock(LockRequest request) {
        LockRequestSizeUtils.validateLockRequestSize(request);
        return delegate().lock(request);
    }
}
