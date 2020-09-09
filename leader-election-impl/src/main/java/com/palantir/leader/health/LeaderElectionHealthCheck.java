/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader.health;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.palantir.leader.LeaderElectionServiceMetrics;
import com.palantir.paxos.Client;

public class LeaderElectionHealthCheck {
    private static final double MAX_ALLOWED_LAST_5_MINUTE_RATE = 0.015;
    private final ConcurrentMap<Client, LeaderElectionServiceMetrics> clientWiseMetrics = new ConcurrentHashMap<>();

    public void registerClient(Client namespace, LeaderElectionServiceMetrics leaderElectionServiceMetrics) {
        clientWiseMetrics.putIfAbsent(namespace, leaderElectionServiceMetrics);
    }

    private double getLeaderElectionRateForAllClients() {
        return clientWiseMetrics.values().stream().mapToDouble(this::fiveMinuteRate).sum();
    }

    private double fiveMinuteRate(LeaderElectionServiceMetrics leaderElectionRateForClient) {
        return leaderElectionRateForClient.proposedLeadership().getFiveMinuteRate();
    }

    public LeaderElectionHealthStatus leaderElectionRateHealthStatus() {
        return getLeaderElectionRateForAllClients() <= MAX_ALLOWED_LAST_5_MINUTE_RATE
                ? LeaderElectionHealthStatus.HEALTHY : LeaderElectionHealthStatus.UNHEALTHY;
    }
}
