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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.immutables.value.Value;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.InProgressResponseState;
import com.palantir.paxos.PaxosQuorumChecker;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster implements TestRule {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String clusterName;
    private final List<TemporaryConfigurationHolder> configs;
    private final Set<TestableTimelockServer> servers;
    private final Multimap<TestableTimelockServer, TestableTimelockServer> serverToOtherServers;
    private final FailoverProxyFactory proxyFactory;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Map<String, NamespacedClients> clientsByNamespace = Maps.newConcurrentMap();

    public TestableTimelockCluster(String baseUri, String... configFileTemplates) {
        this(ClusterName.random(), baseUri, configFileTemplates);
    }

    public TestableTimelockCluster(ClusterName clusterName, String baseUri, String... configFileTemplates) {
        this.clusterName = clusterName.get();
        this.configs = Arrays.stream(configFileTemplates)
                .map(this::getConfigHolder)
                .collect(Collectors.toList());
        this.servers = configs.stream()
                .map(TestableTimelockCluster::getServerHolder)
                .map(holder -> new TestableTimelockServer(baseUri, holder))
                .collect(Collectors.toSet());
        this.serverToOtherServers = KeyedStream.of(servers)
                .map(server -> ImmutableSet.of(server))
                .map(server -> Sets.difference(servers, server))
                .flatMap(Collection::stream)
                .collectToSetMultimap();
        this.proxyFactory = new FailoverProxyFactory(new TestProxies(baseUri, ImmutableList.copyOf(servers)));
    }

    @Value.Immutable
    public abstract static class ClusterName {
        @Value.Parameter
        public abstract String get();

        @Override
        public String toString() {
            return get();
        }

        static ClusterName random() {
            return ImmutableClusterName.of(Hashing.murmur3_32().hashLong(new Random().nextLong()).toString());
        }
    }

    void waitUntilLeaderIsElected(List<String> namespaces) {
        waitUntilReadyToServeNamespaces(namespaces);
    }

    private void waitUntilReadyToServeNamespaces(List<String> namespaces) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        namespaces.forEach(name -> client(name).getFreshTimestamp());
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    void waitUntilAllServersOnlineAndReadyToServeNamespaces(List<String> namespaces) {
        servers.forEach(TestableTimelockServer::start);
        waitUntilReadyToServeNamespaces(namespaces);
    }

    TestableTimelockServer currentLeaderFor(String namespace) {
        return Iterables.getOnlyElement(currentLeaders(namespace).get(namespace));
    }

    SetMultimap<String, TestableTimelockServer> currentLeaders(String... namespaces) {
        Set<String> namespacesIterable = ImmutableSet.copyOf(namespaces);
        KeyedStream<TestableTimelockServer, PaxosContainer<Set<String>>> responses = PaxosQuorumChecker.collectUntil(
                ImmutableList.copyOf(servers),
                server -> PaxosContainer.of(server.pinger().ping(namespaces)),
                Maps.toMap(servers, unused -> executorService),
                Duration.ofSeconds(2),
                untilAllNamespacesAreSeen(namespacesIterable))
                .stream();

        return responses
                .filter(PaxosContainer::isSuccessful)
                .map(PaxosContainer::get)
                .flatMap(Collection::stream)
                .mapEntries((server, namespace) -> Maps.immutableEntry(namespace, server))
                .collectToSetMultimap();
    }

    private static Predicate<InProgressResponseState<TestableTimelockServer, PaxosContainer<Set<String>>>>
    untilAllNamespacesAreSeen(Set<String> namespacesIterable) {
        return state -> state.responses().values().stream()
                .filter(PaxosContainer::isSuccessful)
                .map(PaxosContainer::get)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())
                .containsAll(namespacesIterable);
    }


    SetMultimap<String, TestableTimelockServer> nonLeaders(String... namespaces) {
        SetMultimap<String, TestableTimelockServer> currentLeaderPerNamespace = currentLeaders(namespaces);

        assertThat(currentLeaderPerNamespace.asMap().values())
                .as("there should only be one leader per namespace")
                .allMatch(servers -> servers.size() == 1);

        return KeyedStream.stream(currentLeaderPerNamespace)
                .flatMap(leader -> serverToOtherServers.get(leader).stream())
                .collectToSetMultimap();
    }

    void failoverToNewLeader(String namespace) {
        int maxTries = 5;
        for (int i = 0; i < maxTries; i++) {
            if (tryFailoverToNewLeader(namespace)) {
                return;
            }
        }

        throw new IllegalStateException("unable to force a failover after " + maxTries + " tries");
    }

    private boolean tryFailoverToNewLeader(String namespace) {
        TestableTimelockServer leader = currentLeaderFor(namespace);
        leader.kill();
        waitUntilLeaderIsElected(ImmutableList.of(namespace));
        leader.start();

        return !currentLeaderFor(namespace).equals(leader);
    }

    Set<TestableTimelockServer> servers() {
        return servers;
    }

    NamespacedClients clientForRandomNamespace() {
        return client(UUID.randomUUID().toString());
    }

    NamespacedClients client(String namespace) {
        return clientsByNamespace.computeIfAbsent(namespace, this::uncachedNamespacedClients);
    }

    NamespacedClients uncachedNamespacedClients(String namespace) {
        return NamespacedClients.from(namespace, proxyFactory);
    }

    private static final class FailoverProxyFactory implements NamespacedClients.ProxyFactory {

        private final TestProxies proxies;

        private FailoverProxyFactory(TestProxies proxies) {
            this.proxies = proxies;
        }

        @Override
        public <T> T createProxy(Class<T> clazz) {
            return proxies.failover(clazz);
        }
    }

    RuleChain getRuleChain() {
        RuleChain ruleChain = RuleChain.outerRule(temporaryFolder);

        for (TemporaryConfigurationHolder config : configs) {
            ruleChain = ruleChain.around(config);
        }

        for (TestableTimelockServer server : servers) {
            ruleChain = ruleChain.around(server.serverHolder());
        }

        return ruleChain;
    }

    private static TimeLockServerHolder getServerHolder(TemporaryConfigurationHolder configHolder) {
        return new TimeLockServerHolder(configHolder::getTemporaryConfigFileLocation);
    }

    private TemporaryConfigurationHolder getConfigHolder(String configFileName) {
        File configTemplate = new File(ResourceHelpers.resourceFilePath(configFileName));
        return new TemporaryConfigurationHolder(temporaryFolder, configTemplate);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return getRuleChain().apply(base, description);
    }

    @Override
    public String toString() {
        return clusterName;
    }
}
