/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;

// In-memory alternative to DefaultNamespacedTimelockRpcClient - doesn't actually make RPCs
public class InMemoryNamespacedTimelockRpcClient implements NamespacedTimelockRpcClient {
    private final AsyncTimelockService delegate;

    public InMemoryNamespacedTimelockRpcClient(AsyncTimelockService delegate) {
        this.delegate = delegate;
    }

    @Override
    public long getImmutableTimestamp() {
        return delegate.getImmutableTimestamp();
    }

    @Override
    public long currentTimeMillis() {
        return delegate.currentTimeMillis();
    }
}
