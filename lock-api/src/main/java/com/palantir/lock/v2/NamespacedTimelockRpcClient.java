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

package com.palantir.lock.v2;

/**
 * Adapter that mirrors {@code TimelockRpcClient}, but for convenience automatically provides the namespace as a
 * parameter.
 */
public class NamespacedTimelockRpcClient {
    private final TimelockRpcClient timelockRpcClient;
    private final String namespace;

    public NamespacedTimelockRpcClient(TimelockRpcClient timelockRpcClient, String namespace) {
        this.timelockRpcClient = timelockRpcClient;
        this.namespace = namespace;
    }

    public long getImmutableTimestamp() {
        return timelockRpcClient.getImmutableTimestamp(namespace);
    }

    public long currentTimeMillis() {
        return timelockRpcClient.currentTimeMillis(namespace);
    }
}
