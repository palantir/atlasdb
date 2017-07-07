/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String defaultClient;
    private final String baseUri;
    private final List<TemporaryConfigurationHolder> configs;
    private final List<TestableTimelockServer> servers;
    private final TestProxies proxies;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    public TestableTimelockCluster(String baseUri, String defaultClient, String... configFileTemplates) {
        this.defaultClient = defaultClient;
        this.baseUri = baseUri;
        this.configs = Arrays.asList(configFileTemplates).stream()
                .map(this::getConfigHolder)
                .collect(Collectors.toList());
        this.servers = configs.stream()
                .map(this::getServerHolder)
                .map(holder -> new TestableTimelockServer(baseUri, defaultClient, holder))
                .collect(Collectors.toList());
        this.proxies = new TestProxies(baseUri, servers);
    }

    public void waitUntilLeaderIsElected() {
        TimestampService timestampService = timestampService();
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        timestampService.getFreshTimestamp();
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    public void waitUntillAllSeversAreOnlineAndLeaderIsElected() {
        servers.forEach(TestableTimelockServer::start);
        waitUntilLeaderIsElected();
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

    public LockRefreshToken remoteLock(String client, LockRequest lockRequest) throws InterruptedException {
        return lockService().lock(client, lockRequest);
    }

    public boolean remoteUnlock(LockRefreshToken token) {
        return lockService().unlock(token);
    }

    public LockTokenV2 lock(LockRequestV2 requestV2) {
        return timelockService().lock(requestV2);
    }

    public CompletableFuture<LockTokenV2> lockAsync(LockRequestV2 requestV2) {
        return CompletableFuture.supplyAsync(() -> lock(requestV2), executor);
    }

    public boolean unlock(LockTokenV2 token) {
        return timelockService().unlock(ImmutableSet.of(token)).contains(token);
    }

    public Set<LockTokenV2> refreshLockLeases(Set<LockTokenV2> tokens) {
        return timelockService().refreshLockLeases(tokens);
    }

    public boolean refreshLockLease(LockTokenV2 token) {
        return refreshLockLeases(ImmutableSet.of(token)).contains(token);
    }

    public void waitForLocks(WaitForLocksRequest request) {
        timelockService().waitForLocks(request);
    }

    public CompletableFuture<Void> waitForLocksAsync(WaitForLocksRequest request) {
        return CompletableFuture.runAsync(() -> waitForLocks(request), executor);
    }

    public TimestampService timestampService() {
        return proxies.failoverForClient(defaultClient, TimestampService.class);
    }

    public RemoteLockService lockService() {
        return proxies.failoverForClient(defaultClient, RemoteLockService.class);
    }

    public TimelockService timelockService() {
        return proxies.failoverForClient(defaultClient, TimelockService.class);
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
