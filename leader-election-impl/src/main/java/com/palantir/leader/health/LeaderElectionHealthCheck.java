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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.palantir.leader.LeaderElectionServiceMetrics;
import com.palantir.paxos.Client;

public class LeaderElectionHealthCheck {
    public static final double MAX_ALLOWED_LAST_5_MINUTE_RATE = 0.015;

//    The first mark on leader proposal metric causes spike in 5 min rate and the health check inaccurately
//    becomes unhealthy. We deactivate the health check at start up until the initial mark has negligible
//    weight in the last 5 min rate.
    private static final Duration HEALTH_CHECK_DEACTIVATION_PERIOD = Duration.ofMinutes(14);

    private final ConcurrentMap<Client, LeaderElectionServiceMetrics> clientWiseMetrics = new ConcurrentHashMap<>();
    private volatile Instant timeFirstClientRegistered = Instant.now();
    private volatile boolean healthCheckDeactivated = true;

    public void registerClient(Client namespace, LeaderElectionServiceMetrics leaderElectionServiceMetrics) {
        updateDeactivationStartTime();
        clientWiseMetrics.putIfAbsent(namespace, leaderElectionServiceMetrics);
    }

    public void updateDeactivationStartTime() {
        timeFirstClientRegistered = clientWiseMetrics.isEmpty() ? Instant.now() : timeFirstClientRegistered;
    }

    private double getLeaderElectionRateForAllClients() {
        return clientWiseMetrics.values().stream().mapToDouble(this::fiveMinuteRate).sum();
    }

    private double fiveMinuteRate(LeaderElectionServiceMetrics leaderElectionRateForClient) {
        return leaderElectionRateForClient.proposedLeadership().getFiveMinuteRate();
    }

    private boolean isHealthCheckDeactivated() {
        if (!healthCheckDeactivated) {
            return false;
        }
        boolean shouldBeDeactivated = healthCheckDeactivated && isWithinDeactivationWindow();
        healthCheckDeactivated = shouldBeDeactivated;
        return shouldBeDeactivated;
    }

    private boolean isWithinDeactivationWindow() {
        return clientWiseMetrics.isEmpty()
                || Duration.between(timeFirstClientRegistered, Instant.now()).compareTo(HEALTH_CHECK_DEACTIVATION_PERIOD) < 0;
    }

    private boolean isHealthy(double leaderElectionRateForAllClients) {
        return isHealthCheckDeactivated() || (leaderElectionRateForAllClients <= MAX_ALLOWED_LAST_5_MINUTE_RATE);
    }

    public LeaderElectionHealthReport leaderElectionRateHealthReport() {
        double leaderElectionRateForAllClients = getLeaderElectionRateForAllClients();
        LeaderElectionHealthStatus status = isHealthy(leaderElectionRateForAllClients)
                ? LeaderElectionHealthStatus.HEALTHY : LeaderElectionHealthStatus.UNHEALTHY;
        return LeaderElectionHealthReport.builder()
                .status(status)
                .leaderElectionRate(leaderElectionRateForAllClients)
                .build();
    }
}
