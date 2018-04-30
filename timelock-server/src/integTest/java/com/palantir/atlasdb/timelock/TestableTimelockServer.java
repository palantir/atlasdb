/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.io.IOException;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class TestableTimelockServer {

    private final TimeLockServerHolder serverHolder;
    private final String defaultClient;
    private final TestProxies proxies;

    public TestableTimelockServer(String baseUri, String defaultClient, TimeLockServerHolder serverHolder) {
        this.defaultClient = defaultClient;
        this.serverHolder = serverHolder;
        this.proxies = new TestProxies(baseUri, ImmutableList.of());
    }

    public TimeLockServerHolder serverHolder() {
        return serverHolder;
    }

    public boolean leaderPing() {
        return pingableLeader().ping();
    }

    public long getFreshTimestamp() {
        return timestampService().getFreshTimestamp();
    }

    public LockRefreshToken remoteLock(String client, com.palantir.lock.LockRequest lockRequest)
            throws InterruptedException {
        return lockService().lock(client, lockRequest);
    }

    public LockResponse lock(LockRequest lockRequest) {
        return timelockService().lock(lockRequest);
    }

    public void kill() {
        serverHolder.kill();
    }

    public void start() {
        serverHolder.start();
    }

    public PingableLeader pingableLeader() {
        return proxies.singleNode(serverHolder, PingableLeader.class);
    }

    public TimestampService timestampService() {
        return proxies.singleNodeForClient(defaultClient, serverHolder, TimestampService.class);
    }

    public TimestampManagementService timestampManagementService() {
        return proxies.singleNodeForClient(defaultClient, serverHolder, TimestampManagementService.class);
    }

    public LockService lockService() {
        return proxies.singleNodeForClient(defaultClient, serverHolder, LockService.class);
    }

    public TimelockService timelockService() {
        return timelockServiceForClient(defaultClient);
    }

    public TimelockService timelockServiceForClient(String client) {
        return proxies.singleNodeForClient(client, serverHolder, TimelockService.class);
    }

    public MetricsOutput getMetricsOutput() {
        try {
            return new MetricsOutput(
                    new ObjectMapper().readTree(
                            new URL("http", "localhost", serverHolder.getAdminPort(), "/metrics")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
