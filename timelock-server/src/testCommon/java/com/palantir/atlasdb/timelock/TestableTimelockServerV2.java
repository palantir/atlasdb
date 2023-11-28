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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.timelock.NamespacedClientsV2.ProxyFactoryV2;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.atlasdb.timelock.paxos.BatchPingableLeader;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.api.NamespaceLeadershipTakeoverService;
import com.palantir.atlasdb.timelock.util.TestProxiesV2;
import com.palantir.atlasdb.timelock.util.TestProxiesV2.ProxyModeV2;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.Client;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import com.palantir.tokens.auth.AuthHeader;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestableTimelockServerV2 {

    private static final ImmutableSet<Client> PSEUDO_LEADERSHIP_CLIENT_SET =
            ImmutableSet.of(PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT);
    private final TimeLockServerHolderV2 serverHolder;
    private final TestProxiesV2 proxies;
    private final ProxyFactoryV2 proxyFactory;

    private final Map<String, NamespacedClientsV2> clientsByNamespace = new ConcurrentHashMap<>();
    private volatile boolean switchToBatched = false;

    TestableTimelockServerV2(String baseUri, TimeLockServerHolderV2 serverHolder) {
        this.serverHolder = serverHolder;
        this.proxies = new TestProxiesV2(baseUri, ImmutableList.of());
        this.proxyFactory = new SingleNodeProxyFactory(proxies, serverHolder);
    }

    public TimeLockServerHolderV2 serverHolder() {
        return serverHolder;
    }

    ListenableFuture<Void> killAsync() {
        ListenableFuture<Void> kill = serverHolder.kill();
        kill.addListener(
                () -> {
                    // https://github.com/palantir/dialogue/issues/1119
                    clientsByNamespace.clear();
                    proxies.clearProxies();
                },
                MoreExecutors.directExecutor());
        return kill;
    }

    void killSync() {
        Futures.getUnchecked(killAsync());
    }

    void start() {
        serverHolder.start();
    }

    void startUsingBatchedSingleLeader() {
        switchToBatched = true;
    }

    void stopUsingBatchedSingleLeader() {
        switchToBatched = false;
    }

    TestableLeaderPinger pinger() {
        PaxosLeaderMode mode = serverHolder.installConfig().paxos().leaderMode();

        if (switchToBatched) {
            BatchPingableLeader batchPingableLeader =
                    proxies.singleNode(serverHolder, BatchPingableLeader.class, false, ProxyModeV2.DIRECT);
            return namespaces ->
                    batchPingableLeader.ping(PSEUDO_LEADERSHIP_CLIENT_SET).isEmpty()
                            ? ImmutableSet.of()
                            : ImmutableSet.copyOf(namespaces);
        }

        switch (mode) {
            case SINGLE_LEADER:
                PingableLeader pingableLeader =
                        proxies.singleNode(serverHolder, PingableLeader.class, false, ProxyModeV2.DIRECT);
                return namespaces -> {
                    if (pingableLeader.ping()) {
                        return ImmutableSet.copyOf(namespaces);
                    } else {
                        return ImmutableSet.of();
                    }
                };
            case LEADER_PER_CLIENT:
                BatchPingableLeader batchPingableLeader =
                        proxies.singleNode(serverHolder, BatchPingableLeader.class, false, ProxyModeV2.DIRECT);
                return namespaces -> {
                    Set<Client> typedNamespaces =
                            Streams.stream(namespaces).map(Client::of).collect(Collectors.toSet());

                    return batchPingableLeader.ping(typedNamespaces).stream()
                            .map(Client::value)
                            .collect(Collectors.toSet());
                };
            case AUTO_MIGRATION_MODE:
                throw new UnsupportedOperationException("auto migration mode isn't supported just yet");
        }

        throw new SafeIllegalStateException("unexpected mode", SafeArg.of("mode", mode));
    }

    public boolean isMultiLeader() {
        PaxosLeaderMode mode = serverHolder.installConfig().paxos().leaderMode();
        return mode == PaxosLeaderMode.LEADER_PER_CLIENT;
    }

    NamespacedClientsV2 client(String namespace) {
        return clientsByNamespace.computeIfAbsent(namespace, key -> NamespacedClientsV2.from(namespace, proxyFactory));
    }

    public TaggedMetricRegistry taggedMetricRegistry() {
        return serverHolder.getTaggedMetricsRegistry();
    }

    @Override
    public String toString() {
        return "TestableTimelockServer{url='" + serverHolder.getTimelockUri() + "'}";
    }

    void rejectAllNamespacesOtherThan(Iterable<String> namespacesToAccept) {
        StubMapping failEverything = any(urlMatching(TimeLockServerHolderV2.ALL_NAMESPACES))
                .willReturn(aResponse().withStatus(503))
                .atPriority(Integer.MAX_VALUE - 1)
                .build();
        serverHolder.wireMock().register(failEverything);

        Streams.stream(namespacesToAccept)
                .flatMap(namespace -> Stream.of(
                        any(namespaceEqualTo(namespace)),
                        any(conjureUrlNamespaceEqualTo(namespace)),
                        any(legacyLockUrlNamespaceEqualTo(namespace))))
                .map(this::namespacesIsProxiedToTimelock)
                .forEach(serverHolder.wireMock()::register);
    }

    void allowAllNamespaces() {
        serverHolder.resetWireMock();
    }

    private static UrlPattern namespaceEqualTo(String namespace) {
        return urlMatching(String.format("/%s/.*", namespace));
    }

    private static UrlPattern conjureUrlNamespaceEqualTo(String namespace) {
        return urlMatching(String.format("/tl/.*/%s", namespace));
    }

    private static UrlPattern legacyLockUrlNamespaceEqualTo(String namespace) {
        return urlMatching(String.format("/lk/.*/%s", namespace));
    }

    private StubMapping namespacesIsProxiedToTimelock(MappingBuilder mappingBuilder) {
        return mappingBuilder
                .willReturn(aResponse()
                        .proxiedFrom(serverHolder.getTimelockUri())
                        .withAdditionalRequestHeader(
                                "User-Agent", UserAgents.format(TimeLockServerHolderV2.WIREMOCK_USER_AGENT)))
                .atPriority(1)
                .build();
    }

    public boolean takeOverLeadershipForNamespace(String namespace) {
        return proxies.singleNode(serverHolder, NamespaceLeadershipTakeoverService.class, ProxyModeV2.DIRECT)
                .takeover(AuthHeader.valueOf("omitted"), namespace);
    }

    public TimeLockManagementService timeLockManagementService() {
        return proxies.singleNode(serverHolder, TimeLockManagementService.class, ProxyModeV2.WIREMOCK);
    }

    public MultiClientConjureTimelockService multiClientService() {
        return proxies.singleNode(serverHolder, MultiClientConjureTimelockService.class, ProxyModeV2.WIREMOCK);
    }

    private static final class SingleNodeProxyFactory implements ProxyFactoryV2 {

        private final TestProxiesV2 proxies;
        private final TimeLockServerHolderV2 serverHolder;

        private SingleNodeProxyFactory(TestProxiesV2 proxies, TimeLockServerHolderV2 serverHolder) {
            this.proxies = proxies;
            this.serverHolder = serverHolder;
        }

        @Override
        public <T> T createProxy(Class<T> clazz, ProxyModeV2 proxyMode) {
            return proxies.singleNode(serverHolder, clazz, proxyMode);
        }
    }
}
