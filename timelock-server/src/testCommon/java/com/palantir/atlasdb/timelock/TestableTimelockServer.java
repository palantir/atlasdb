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
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.NamespacedClients.ProxyFactory;
import com.palantir.atlasdb.timelock.paxos.BatchPingableLeader;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

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

    ListenableFuture<Void> killAsync() {
        return serverHolder.kill();
    }

    void killSync() {
        Futures.getUnchecked(killAsync());
    }

    void start() {
        serverHolder.start();
    }

    TestableLeaderPinger pinger() {
        PaxosLeaderMode mode = serverHolder.installConfig().paxos().leaderMode();

        switch (mode) {
            case SINGLE_LEADER:
                PingableLeader pingableLeader = proxies.singleNode(serverHolder, PingableLeader.class, false);
                return namespaces -> {
                    if (pingableLeader.ping()) {
                        return ImmutableSet.copyOf(namespaces);
                    } else {
                        return ImmutableSet.of();
                    }
                };
            case LEADER_PER_CLIENT:
                BatchPingableLeader batchPingableLeader =
                        proxies.singleNode(serverHolder, BatchPingableLeader.class, false);
                return namespaces -> {
                    Set<Client> typedNamespaces = Streams.stream(namespaces)
                            .map(Client::of)
                            .collect(Collectors.toSet());

                    return batchPingableLeader.ping(typedNamespaces).stream()
                            .map(Client::value)
                            .collect(Collectors.toSet());
                };
            case AUTO_MIGRATION_MODE:
                throw new UnsupportedOperationException("auto migration mode isn't supported just yet");
        }

        throw new SafeIllegalStateException("unexpected mode", SafeArg.of("mode", mode));
    }

    NamespacedClients client(String namespace) {
        return clientsByNamespace.computeIfAbsent(namespace, key -> NamespacedClients.from(namespace, proxyFactory));
    }

    public TaggedMetricRegistry taggedMetricRegistry() {
        return serverHolder.getTaggedMetricsRegistry();
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
