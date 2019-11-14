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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.immutables.value.Value;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.palantir.atlasdb.timelock.util.TestProxies;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster implements TestRule {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String clusterName;
    private final List<TemporaryConfigurationHolder> configs;
    private final List<TestableTimelockServer> servers;
    private final FailoverProxyFactory proxyFactory;

    private final Map<String, NamespacedClients> clientsByNamespace = Maps.newConcurrentMap();

    @Nullable
    private TestableTimelockServer lastLeader;

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
                .collect(Collectors.toList());
        this.proxyFactory = new FailoverProxyFactory(new TestProxies(baseUri, servers));
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

    void waitUntilLeaderIsElected() {
        waitUntilLeaderIsElected(ImmutableList.of(UUID.randomUUID().toString()));
    }

    void waitUntilLeaderIsElected(List<String> clients) {
        waitUntilReadyToServeClients(clients);
    }

    private void waitUntilReadyToServeClients(List<String> clients) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        clients.forEach(name -> client(name).getFreshTimestamp());
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    void waitUntilAllServersOnlineAndReadyToServeClients(List<String> additionalClients) {
        servers.forEach(TestableTimelockServer::start);
        waitUntilReadyToServeClients(additionalClients);
    }

    TestableTimelockServer currentLeader() {
        if (lastLeader != null && lastLeader.pingableLeader().ping()) {
            return lastLeader;
        }

        for (TestableTimelockServer server : servers) {
            try {
                if (server.pingableLeader().ping()) {
                    lastLeader = server;
                    return server;
                }
            } catch (Throwable t) {
                // continue;
            }
        }
        throw new IllegalStateException("no nodes are currently the leader");
    }

    List<TestableTimelockServer> nonLeaders() {
        TestableTimelockServer leader = currentLeader();
        return servers.stream()
                .filter(server -> server != leader)
                .collect(Collectors.toList());
    }

    void failoverToNewLeader() {
        int maxTries = 5;
        for (int i = 0; i < maxTries; i++) {
            if (tryFailoverToNewLeader()) {
                return;
            }
        }

        throw new IllegalStateException("unable to force a failover after " + maxTries + " tries");
    }

    private boolean tryFailoverToNewLeader() {
        TestableTimelockServer leader = currentLeader();
        leader.kill();
        waitUntilLeaderIsElected();
        leader.start();

        return !currentLeader().equals(leader);
    }

    List<TestableTimelockServer> servers() {
        return servers;
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
