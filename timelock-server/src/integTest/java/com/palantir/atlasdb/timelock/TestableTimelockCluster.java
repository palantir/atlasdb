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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRpcClient;
import com.palantir.lock.LockService;
import com.palantir.lock.client.AsyncTimeLockUnlocker;
import com.palantir.lock.client.RemoteLockServiceAdapter;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.client.TimeLockUnlocker;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.NamespaceAwareTimelockRpcClient;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String client = UUID.randomUUID().toString();
    private final List<TemporaryConfigurationHolder> configs;
    private final List<TestableTimelockServer> servers;
    private final Map<String, TimelockService> timelockServicesForClient = Maps.newConcurrentMap();
    private final Map<String, TimeLockUnlocker> unlockerForClient = Maps.newConcurrentMap();
    private final TestProxies proxies;

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    TestableTimelockCluster(String baseUri, String... configFileTemplates) {
        this.configs = Arrays.stream(configFileTemplates)
                .map(this::getConfigHolder)
                .collect(Collectors.toList());
        this.servers = configs.stream()
                .map(TestableTimelockCluster::getServerHolder)
                .map(holder -> new TestableTimelockServer(baseUri, client, holder))
                .collect(Collectors.toList());
        this.proxies = new TestProxies(baseUri, servers);
    }

    void waitUntilLeaderIsElected() {
        waitUntilLeaderIsElected(ImmutableList.of());
    }

    void waitUntilLeaderIsElected(List<String> additionalClients) {
        List<String> clients = new ArrayList<>(additionalClients);
        clients.add(client);
        waitUntilReadyToServeClients(clients);
    }

    private void waitUntilReadyToServeClients(List<String> clients) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        clients.forEach(name -> timelockServiceForClient(name).getFreshTimestamp());
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
        for (TestableTimelockServer server : servers) {
            try {
                if (server.leaderPing()) {
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

    long getFreshTimestamp() {
        return timestampService().getFreshTimestamp();
    }

    TimestampRange getFreshTimestamps(int number) {
        return timestampService().getFreshTimestamps(number);
    }

    LockRefreshToken remoteLock(com.palantir.lock.LockRequest lockRequest)
            throws InterruptedException {
        return lockService().lock(client, lockRequest);
    }

    public boolean remoteUnlock(LockRefreshToken token) {
        return lockService().unlock(token);
    }

    public LockResponse lock(LockRequest requestV2) {
        return timelockService().lock(requestV2);
    }

    CompletableFuture<LockResponse> lockAsync(LockRequest requestV2) {
        return CompletableFuture.supplyAsync(() -> lock(requestV2), executor);
    }

    public boolean unlock(LockToken token) {
        return timelockService().unlock(ImmutableSet.of(token)).contains(token);
    }

    private Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return timelockService().refreshLockLeases(tokens);
    }

    boolean refreshLockLease(LockToken token) {
        return refreshLockLeases(ImmutableSet.of(token)).contains(token);
    }

    WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return timelockService().waitForLocks(request);
    }

    CompletableFuture<WaitForLocksResponse> waitForLocksAsync(WaitForLocksRequest request) {
        return CompletableFuture.supplyAsync(() -> waitForLocks(request), executor);
    }

    StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(
            StartIdentifiedAtlasDbTransactionRequest request) {
        return namespaceAwareClient().deprecatedStartTransaction(request).toStartTransactionResponse();
    }

    private TimestampService timestampService() {
        return proxies.failoverForClient(client, TimestampService.class);
    }

    LockService lockService() {
        return RemoteLockServiceAdapter.create(proxies.failover(LockRpcClient.class, proxies.getServerUris()), client);
    }

    TimelockService timelockService() {
        return timelockServiceForClient(client);
    }

    TimelockService timelockServiceForClient(String name) {
        return timelockServicesForClient.computeIfAbsent(
                name,
                clientName -> RemoteTimelockServiceAdapter.create(
                        proxies.failover(TimelockRpcClient.class, proxies.getServerUris()), clientName));
    }

    TimeLockUnlocker unlockerForClient(String name) {
        return unlockerForClient.computeIfAbsent(name,
                clientName -> AsyncTimeLockUnlocker.create(timelockServiceForClient(clientName)));
    }

    <T> CompletableFuture<T> runWithRpcClientAsync(Function<NamespaceAwareTimelockRpcClient, T> function) {
        return CompletableFuture.supplyAsync(() -> function.apply(namespaceAwareClient()));
    }

    NamespaceAwareTimelockRpcClient namespaceAwareClient() {
        return new NamespaceAwareTimelockRpcClient(timelockRpcClient(), client);
    }

    private TimelockRpcClient timelockRpcClient() {
        return proxies.failover(TimelockRpcClient.class, proxies.getServerUris());
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
        File configTemplate =
                new File(ResourceHelpers.resourceFilePath(configFileName));
        return new TemporaryConfigurationHolder(temporaryFolder, configTemplate);
    }

}
