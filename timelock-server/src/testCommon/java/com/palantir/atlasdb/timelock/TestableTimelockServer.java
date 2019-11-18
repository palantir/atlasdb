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
package com.palantir.atlasdb.timelock;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.NamespacedClients.ProxyFactory;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.leader.PingableLeader;

public class TestableTimelockServer {

    private final TimeLockServerHolder serverHolder;
    private final TestProxies proxies;
    private final ProxyFactory proxyFactory;

    private final Map<String, NamespacedClients> clientsByNamespace = Maps.newConcurrentMap();

    TestableTimelockServer(String baseUri, TimeLockServerHolder serverHolder) {
        this.serverHolder = serverHolder;
        this.proxies = new TestProxies(baseUri, ImmutableList.of());
        this.proxyFactory = new SingleNodeProxyFactory(proxies, serverHolder);
    }

    public TimeLockServerHolder serverHolder() {
        return serverHolder;
    }

    void kill() {
        serverHolder.kill();
    }

    void start() {
        serverHolder.start();
    }

    PingableLeader pingableLeader() {
        return proxies.singleNode(serverHolder, PingableLeader.class);
    }

    NamespacedClients client(String namespace) {
        return clientsByNamespace.computeIfAbsent(namespace, key -> NamespacedClients.from(namespace, proxyFactory));
    }

    private static final class SingleNodeProxyFactory implements ProxyFactory {

        private final TestProxies proxies;
        private final TimeLockServerHolder serverHolder;

        private SingleNodeProxyFactory(TestProxies proxies, TimeLockServerHolder serverHolder) {
            this.proxies = proxies;
            this.serverHolder = serverHolder;
        }

        @Override
        public <T> T createProxy(Class<T> clazz) {
            return proxies.singleNode(serverHolder, clazz);
        }
    }

}
