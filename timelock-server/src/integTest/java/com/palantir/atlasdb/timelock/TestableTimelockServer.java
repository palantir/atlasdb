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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class TestableTimelockServer {

    private final String baseUri;
    private final TimeLockServerHolder serverHolder;
    private final String defaultClient;
    private final TestProxies proxies;

    public TestableTimelockServer(String baseUri, String defaultClient, TimeLockServerHolder serverHolder) {
        this.baseUri = baseUri;
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

    public LockRefreshToken lock(String client, LockRequest lockRequest) throws InterruptedException {
        return lockService().lock(client, lockRequest);
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

    public RemoteLockService lockService() {
        return proxies.singleNodeForClient(defaultClient, serverHolder, RemoteLockService.class);
    }

}
