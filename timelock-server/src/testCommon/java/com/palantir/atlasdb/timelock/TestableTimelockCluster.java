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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.atlasdb.timelock.util.TestProxies.ProxyMode;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.InProgressResponseState;
import com.palantir.paxos.PaxosQuorumChecker;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.awaitility.Awaitility;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestableTimelockCluster implements TestRule {

    private static final Logger log = LoggerFactory.getLogger(TestableTimelockCluster.class);
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String name;
    private final List<TemporaryConfigurationHolder> configs;
    private final boolean needsPostgresDatabase;
    private final Set<TestableTimelockServer> servers;
    private final Multimap<TestableTimelockServer, TestableTimelockServer> serverToOtherServers;
    private final FailoverProxyFactory proxyFactory;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final Map<String, NamespacedClients> clientsByNamespace = new ConcurrentHashMap<>();

    public TestableTimelockCluster(String configFileTemplate, TemplateVariables... variables) {
        this(name(), configFileTemplate, variables);
    }

    public TestableTimelockCluster(String name, String configFileTemplate, TemplateVariables... variables) {
        this(name, configFileTemplate, ImmutableList.copyOf(variables));
    }

    public TestableTimelockCluster(String name, String configFileTemplate, Iterable<TemplateVariables> variables) {
        this(
                name,
                configFileTemplate,
                StreamSupport.stream(variables.spliterator(), false)
                        .map(variablesInstance -> ImmutableTestableTimelockServerConfiguration.builder()
                                .templateVariables(variablesInstance)
                                .build())
                        .collect(Collectors.toList()));
    }

    public TestableTimelockCluster(
            String name, String configFileTemplate, List<TestableTimelockServerConfiguration> configurations) {
        this.name = name;
        this.needsPostgresDatabase =
                configurations.stream().anyMatch(TestableTimelockServerConfiguration::needsPostgresDatabase);
        Map<TemplateVariables, TemporaryConfigurationHolder> configMap = KeyedStream.of(configurations)
                .mapKeys(TestableTimelockServerConfiguration::templateVariables)
                .map(configuration -> getConfigHolder(configFileTemplate, configuration.templateVariables()))
                .collectToMap();
        this.configs = ImmutableList.copyOf(configMap.values());
        this.servers = ImmutableSet.copyOf(KeyedStream.stream(configMap)
                .mapEntries((template, holder) -> Maps.immutableEntry(template, getServerHolder(holder, template)))
                .map(holder -> new TestableTimelockServer("https://localhost", holder))
                .collectToMap()
                .values());
        this.serverToOtherServers = KeyedStream.of(servers)
                .map(server -> ImmutableSet.of(server))
                .map(server -> Sets.difference(servers, server))
                .flatMap(Collection::stream)
                .collectToSetMultimap();
        this.proxyFactory =
                new FailoverProxyFactory(new TestProxies("https://localhost", ImmutableList.copyOf(servers)));
    }

    private static String name() {
        return Hashing.murmur3_32().hashLong(new Random().nextLong()).toString();
    }

    boolean isDbTimelock() {
        return needsPostgresDatabase;
    }

    void waitUntilLeaderIsElected(List<String> namespaces) {
        waitUntilReadyToServeNamespaces(namespaces);
    }

    private void waitUntilReadyToServeNamespaces(List<String> namespaces) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    try {
                        namespaces.forEach(namespace ->
                                client(namespace).throughWireMockProxy().getFreshTimestamp());
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

    void killAndAwaitTermination(Iterable<TestableTimelockServer> serversToKill) throws ExecutionException {
        Set<ListenableFuture<Void>> shutdownFutures = ImmutableSet.copyOf(serversToKill).stream()
                .map(TestableTimelockServer::killAsync)
                .collect(Collectors.toSet());

        Futures.getDone(Futures.allAsList(shutdownFutures));
    }

    TestableTimelockServer currentLeaderFor(String namespace) {
        return Iterables.getOnlyElement(currentLeaders(namespace).get(namespace));
    }

    SetMultimap<String, TestableTimelockServer> currentLeaders(String... namespaces) {
        return currentLeaders(ImmutableSet.copyOf(namespaces));
    }

    SetMultimap<String, TestableTimelockServer> currentLeaders(Iterable<String> namespaces) {
        Set<String> namespacesIterable = ImmutableSet.copyOf(namespaces);
        Map<TestableTimelockServer, PaxosContainer<Set<String>>> responses = PaxosQuorumChecker.collectUntil(
                        ImmutableList.copyOf(servers),
                        server -> PaxosContainer.of(server.pinger().ping(namespaces)),
                        Maps.toMap(servers, unused -> new CheckedRejectionExecutorService(executorService)),
                        Duration.ofSeconds(2),
                        untilAllNamespacesAreSeen(namespacesIterable),
                        true)
                .responses();

        log.info("Responses: " + responses);

        SetMultimap<String, TestableTimelockServer> filteredResponses = KeyedStream.stream(responses)
                .filter(PaxosContainer::isSuccessful)
                .map(PaxosContainer::get)
                .flatMap(Collection::stream)
                .mapEntries((server, namespace) -> Maps.immutableEntry(namespace, server))
                .collectToSetMultimap();

        log.info("Filtered responses: " + filteredResponses);

        return filteredResponses;
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
                .allMatch(leadersForNamespace -> leadersForNamespace.size() == 1);

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
        leader.killSync();
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
        public <T> T createProxy(Class<T> clazz, ProxyMode proxyMode) {
            return proxies.failover(clazz, proxyMode);
        }
    }

    RuleChain getRuleChain() {
        RuleChain ruleChain = RuleChain.outerRule(temporaryFolder);

        if (needsPostgresDatabase) {
            ruleChain = ruleChain.around(new DbKvsRule());
        }

        for (TemporaryConfigurationHolder config : configs) {
            ruleChain = ruleChain.around(config);
        }

        for (TestableTimelockServer server : servers) {
            ruleChain = ruleChain.around(server.serverHolder());
        }

        return ruleChain;
    }

    private static TimeLockServerHolder getServerHolder(
            TemporaryConfigurationHolder configHolder, TemplateVariables templateVariables) {
        return new TimeLockServerHolder(configHolder::getTemporaryConfigFileLocation, templateVariables);
    }

    private TemporaryConfigurationHolder getConfigHolder(String templateName, TemplateVariables variables) {
        return new TemporaryConfigurationHolder(temporaryFolder, templateName, variables);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return getRuleChain().apply(base, description);
    }

    @Override
    public String toString() {
        return name;
    }
}
