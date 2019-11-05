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

package com.palantir.lock.watch;

import java.util.Map;
import java.util.UUID;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockWatch;

public class NamespacedLockWatchingRpcClient {
    private final String namespace;
    private final LockWatchingRpcClient lockWatchingRpcClient;

    public NamespacedLockWatchingRpcClient(String namespace, LockWatchingRpcClient lockWatchingRpcClient) {
        this.namespace = namespace;
        this.lockWatchingRpcClient = lockWatchingRpcClient;
    }

    void startWatching(LockWatchRequest lockWatchRequest) {
        lockWatchingRpcClient.startWatching(namespace, lockWatchRequest);
    }

    void stopWatching(LockWatchRequest lockWatchRequest) {
        lockWatchingRpcClient.stopWatching(namespace, lockWatchRequest);
    }

    Map<LockDescriptor, LockWatch> getWatchState(UUID serviceId) {
        return lockWatchingRpcClient.getWatchState(namespace, serviceId);
    }
}
