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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampService;

import io.dropwizard.testing.ResourceHelpers;

public class TestableTimelockCluster {

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String defaultClient;
    private final String baseUri;
    private final List<TemporaryConfigurationHolder> configs;
    private final List<TestableTimelockServer> servers;
    private final TestProxies proxies;

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
        getFreshTimestamp();
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
        TestableTimelockServer leader = currentLeader();
        leader.kill();
        leader.start();

        waitUntilLeaderIsElected();

        if (servers.size() > 1) {
            assertThat(currentLeader()).isNotEqualTo(leader);
        }
    }

    public List<TestableTimelockServer> servers() {
        return servers;
    }

    public long getFreshTimestamp() {
        return timestampService().getFreshTimestamp();
    }

    public LockRefreshToken lock(String client, LockRequest lockRequest) throws InterruptedException {
        return lockService().lock(client, lockRequest);
    }

    public boolean unlock(LockRefreshToken token) {
        return lockService().unlock(token);
    }

    public TimestampService timestampService() {
        return proxies.failoverForClient(defaultClient, TimestampService.class);
    }

    public RemoteLockService lockService() {
        return proxies.failoverForClient(defaultClient, RemoteLockService.class);
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
