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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.NamespacedClientsV2.ProxyFactoryV2;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.atlasdb.timelock.util.TestProxiesV2;
import com.palantir.atlasdb.timelock.util.TestProxiesV2.ProxyModeV2;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.InProgressResponseState;
import com.palantir.paxos.PaxosQuorumChecker;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestableTimelockClusterV2 implements BeforeAllCallback, AfterAllCallback {

    private final File temporaryFolder;
    private final String name;
    private final List<TemporaryConfigurationHolderV2> configs;
    private final boolean needsPostgresDatabase;
    private final Set<TestableTimelockServerV2> servers;
    private final Multimap<TestableTimelockServerV2, TestableTimelockServerV2> serverToOtherServers;
    private final FailoverProxyFactory proxyFactory;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final List<Extension> extensions = new ArrayList<>();
    private final Map<String, NamespacedClientsV2> clientsByNamespace = new ConcurrentHashMap<>();

    public TestableTimelockClusterV2(String configFileTemplate, TemplateVariables... variables) {
        this(name(), configFileTemplate, variables);
    }

    public TestableTimelockClusterV2(String name, String configFileTemplate, TemplateVariables... variables) {
        this(name, configFileTemplate, ImmutableList.copyOf(variables));
    }

    public TestableTimelockClusterV2(String name, String configFileTemplate, Iterable<TemplateVariables> variables) {
        this(
                name,
                configFileTemplate,
                StreamSupport.stream(variables.spliterator(), false)
                        .map(variablesInstance -> ImmutableTestableTimelockServerConfiguration.builder()
                                .templateVariables(variablesInstance)
                                .build())
                        .collect(Collectors.toList()));
    }

    public TestableTimelockClusterV2(
            String name, String configFileTemplate, List<TestableTimelockServerConfiguration> configurations) {
        try {
            this.temporaryFolder =
                    Files.createTempDirectory("TestableTimelockClusterV2").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.name = name;
        this.needsPostgresDatabase =
                configurations.stream().anyMatch(TestableTimelockServerConfiguration::needsPostgresDatabase);
        Map<TemplateVariables, TemporaryConfigurationHolderV2> configMap = KeyedStream.of(configurations)
                .mapKeys(TestableTimelockServerConfiguration::templateVariables)
                .map(configuration -> getConfigHolder(configFileTemplate, configuration.templateVariables()))
                .collectToMap();
        this.configs = ImmutableList.copyOf(configMap.values());
        this.servers = ImmutableSet.copyOf(KeyedStream.stream(configMap)
                .mapEntries((template, holder) -> Maps.immutableEntry(template, getServerHolder(holder, template)))
                .map(holder -> new TestableTimelockServerV2("https://localhost", holder))
                .collectToMap()
                .values());
        this.serverToOtherServers = KeyedStream.of(servers)
                .map(server -> ImmutableSet.of(server))
                .map(server -> Sets.difference(servers, server))
                .flatMap(Collection::stream)
                .collectToSetMultimap();
        this.proxyFactory =
                new FailoverProxyFactory(new TestProxiesV2("https://localhost", ImmutableList.copyOf(servers)));

        if (needsPostgresDatabase) {
            extensions.add(new DbKvsExtension());
        }
        extensions.addAll(configs);
        for (TestableTimelockServerV2 server : servers) {
            extensions.add(server.serverHolder());
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        for (Extension extension : extensions) {
            if (extension instanceof BeforeAllCallback) {
                ((BeforeAllCallback) extension).beforeAll(extensionContext);
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        for (Extension extension : Lists.reverse(extensions)) {
            if (extension instanceof AfterAllCallback) {
                ((AfterAllCallback) extension).afterAll(extensionContext);
            }
        }
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
        servers.forEach(TestableTimelockServerV2::start);
        waitUntilReadyToServeNamespaces(namespaces);
    }

    void killAndAwaitTermination(Iterable<TestableTimelockServerV2> serversToKill) throws ExecutionException {
        Set<ListenableFuture<Void>> shutdownFutures = ImmutableSet.copyOf(serversToKill).stream()
                .map(TestableTimelockServerV2::killAsync)
                .collect(Collectors.toSet());

        Futures.getDone(Futures.allAsList(shutdownFutures));
    }

    TestableTimelockServerV2 currentLeaderFor(String namespace) {
        return Iterables.getOnlyElement(currentLeaders(namespace).get(namespace));
    }

    SetMultimap<String, TestableTimelockServerV2> currentLeaders(String... namespaces) {
        return currentLeaders(ImmutableSet.copyOf(namespaces));
    }

    SetMultimap<String, TestableTimelockServerV2> currentLeaders(Iterable<String> namespaces) {
        Set<String> namespacesIterable = ImmutableSet.copyOf(namespaces);
        KeyedStream<TestableTimelockServerV2, PaxosContainer<Set<String>>> responses = PaxosQuorumChecker.collectUntil(
                ImmutableList.copyOf(servers),
                server -> PaxosContainer.of(server.pinger().ping(namespaces)),
                Maps.toMap(servers, unused -> new CheckedRejectionExecutorService(executorService)),
                Duration.ofSeconds(2),
                untilAllNamespacesAreSeen(namespacesIterable),
                true)
                .stream();

        return responses
                .filter(PaxosContainer::isSuccessful)
                .map(PaxosContainer::get)
                .flatMap(Collection::stream)
                .mapEntries((server, namespace) -> Maps.immutableEntry(namespace, server))
                .collectToSetMultimap();
    }

    private static Predicate<InProgressResponseState<TestableTimelockServerV2, PaxosContainer<Set<String>>>>
            untilAllNamespacesAreSeen(Set<String> namespacesIterable) {
        return state -> state.responses().values().stream()
                .filter(PaxosContainer::isSuccessful)
                .map(PaxosContainer::get)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet())
                .containsAll(namespacesIterable);
    }

    SetMultimap<String, TestableTimelockServerV2> nonLeaders(String... namespaces) {
        SetMultimap<String, TestableTimelockServerV2> currentLeaderPerNamespace = currentLeaders(namespaces);

        assertThat(currentLeaderPerNamespace.asMap().values())
                .as("there should only be one leader per namespace")
                .allMatch(leadersForNamespace -> leadersForNamespace.size() == 1);

        return KeyedStream.stream(currentLeaderPerNamespace)
                .flatMap(leader -> serverToOtherServers.get(leader).stream())
                .collectToSetMultimap();
    }

    void failoverToNewLeader(String namespace) {
        int maxTries = 8;
        for (int i = 0; i < maxTries; i++) {
            if (tryFailoverToNewLeader(namespace)) {
                return;
            }
        }

        throw new IllegalStateException("unable to force a failover after " + maxTries + " tries");
    }

    private boolean tryFailoverToNewLeader(String namespace) {
        try {
            TestableTimelockServerV2 leader = currentLeaderFor(namespace);
            leader.killSync();
            waitUntilLeaderIsElected(ImmutableList.of(namespace));
            leader.start();

            return !currentLeaderFor(namespace).equals(leader);
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    Set<TestableTimelockServerV2> servers() {
        return servers;
    }

    NamespacedClientsV2 clientForRandomNamespace() {
        return client(UUID.randomUUID().toString());
    }

    NamespacedClientsV2 client(String namespace) {
        return clientsByNamespace.computeIfAbsent(namespace, this::uncachedNamespacedClients);
    }

    NamespacedClientsV2 uncachedNamespacedClients(String namespace) {
        return NamespacedClientsV2.from(namespace, proxyFactory);
    }

    private static final class FailoverProxyFactory implements ProxyFactoryV2 {

        private final TestProxiesV2 proxies;

        private FailoverProxyFactory(TestProxiesV2 proxies) {
            this.proxies = proxies;
        }

        @Override
        public <T> T createProxy(Class<T> clazz, ProxyModeV2 proxyMode) {
            return proxies.failover(clazz, proxyMode);
        }
    }

    private static TimeLockServerHolderV2 getServerHolder(
            TemporaryConfigurationHolderV2 configHolder, TemplateVariables templateVariables) {
        return new TimeLockServerHolderV2(configHolder::getTemporaryConfigFileLocation, templateVariables);
    }

    private TemporaryConfigurationHolderV2 getConfigHolder(String templateName, TemplateVariables variables) {
        return new TemporaryConfigurationHolderV2(temporaryFolder, templateName, variables);
    }

    @Override
    public String toString() {
        return name;
    }
}
