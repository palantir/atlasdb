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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String defaultClient;
    private final List<TemporaryConfigurationHolder> configs;
    private final List<TestableTimelockServer> servers;
    private final TestProxies proxies;

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    public TestableTimelockCluster(String baseUri, String defaultClient, String... configFileTemplates) {
        this.defaultClient = defaultClient;
        this.configs = Arrays.stream(configFileTemplates)
                .map(this::getConfigHolder)
                .collect(Collectors.toList());
        this.servers = configs.stream()
                .map(this::getServerHolder)
                .map(holder -> new TestableTimelockServer(baseUri, defaultClient, holder))
                .collect(Collectors.toList());
        this.proxies = new TestProxies(baseUri, servers);
    }

    public void waitUntilLeaderIsElected() {
        waitUntilReadyToServeClients(ImmutableList.of(defaultClient));
    }

    public void waitUntilReadyToServeClients(List<String> clients) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        clients.forEach(client -> timelockServiceForClient(client).getFreshTimestamp());
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    public void waitUntilAllServersOnlineAndReadyToServeClients(List<String> clients) {
        servers.forEach(TestableTimelockServer::start);
        waitUntilReadyToServeClients(clients);
    }

    public TestableTimelockServer currentLeader() {
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

    public List<TestableTimelockServer> nonLeaders() {
        TestableTimelockServer leader = currentLeader();
        return servers.stream()
                .filter(server -> server != leader)
                .collect(Collectors.toList());
    }

    public void failoverToNewLeader() {
        if (servers.size() == 1) {
            // if there is only one server, it's going to become the leader again
            tryFailoverToNewLeader();
            return;
        }

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
        Uninterruptibles.sleepUninterruptibly(1_000, TimeUnit.MILLISECONDS);
        leader.start();

        waitUntilLeaderIsElected();

        return !currentLeader().equals(leader);
    }

    public List<TestableTimelockServer> servers() {
        return servers;
    }

    public long getFreshTimestamp() {
        return timestampService().getFreshTimestamp();
    }

    public TimestampRange getFreshTimestamps(int number) {
        return timestampService().getFreshTimestamps(number);
    }

    public LockRefreshToken remoteLock(String client, com.palantir.lock.LockRequest lockRequest)
            throws InterruptedException {
        return lockService().lock(client, lockRequest);
    }

    public boolean remoteUnlock(LockRefreshToken token) {
        return lockService().unlock(token);
    }

    public LockResponse lock(LockRequest requestV2) {
        return timelockService().lock(requestV2);
    }

    public CompletableFuture<LockResponse> lockAsync(LockRequest requestV2) {
        return CompletableFuture.supplyAsync(() -> lock(requestV2), executor);
    }

    public boolean unlock(LockToken token) {
        return timelockService().unlock(ImmutableSet.of(token)).contains(token);
    }

    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return timelockService().refreshLockLeases(tokens);
    }

    public boolean refreshLockLease(LockToken token) {
        return refreshLockLeases(ImmutableSet.of(token)).contains(token);
    }

    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return timelockService().waitForLocks(request);
    }

    public CompletableFuture<WaitForLocksResponse> waitForLocksAsync(WaitForLocksRequest request) {
        return CompletableFuture.supplyAsync(() -> waitForLocks(request), executor);
    }

    public StartAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(StartAtlasDbTransactionRequest request) {
        return timelockService().startIdentifiedAtlasDbTransaction(request);
    }

    public TimestampService timestampService() {
        return proxies.failoverForClient(defaultClient, TimestampService.class);
    }

    public LockService lockService() {
        return proxies.failoverForClient(defaultClient, LockService.class);
    }

    public TimelockService timelockService() {
        return proxies.failoverForClient(defaultClient, TimelockService.class);
    }

    public TimelockService timelockServiceForClient(String client) {
        return proxies.failoverForClient(client, TimelockService.class);
    }

    public RuleChain getRuleChain() {
        RuleChain ruleChain = RuleChain.outerRule(temporaryFolder);

        for (TemporaryConfigurationHolder config : configs) {
            ruleChain = ruleChain.around(config);
        }

        for (TestableTimelockServer server : servers) {
            ruleChain = ruleChain.around(server.serverHolder());
        }

        return ruleChain;
    }

    private TimeLockServerHolder getServerHolder(TemporaryConfigurationHolder configHolder) {
        return new TimeLockServerHolder(configHolder::getTemporaryConfigFileLocation);
    }

    private TemporaryConfigurationHolder getConfigHolder(String configFileName) {
        File configTemplate =
                new File(ResourceHelpers.resourceFilePath(configFileName));
        return new TemporaryConfigurationHolder(temporaryFolder, configTemplate);
    }

}
