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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.LeaderElectionServiceMetrics;
import com.palantir.paxos.Client;

public class LeaderElectionHealthCheck {
    // todo sudiksha
    private static final Logger log = LoggerFactory.getLogger(LeaderElectionHealthCheck.class);
    private static final double MAX_ALLOWED_LAST_5_MINUTE_RATE = 0.0095;
    private static final ConcurrentMap<Client, LeaderElectionServiceMetrics> clientWiseMetrics = new ConcurrentHashMap<>();


    public static LeaderElectionServiceMetrics registerClient(Client namespace,
            LeaderElectionServiceMetrics leaderElectionServiceMetrics) {
        return clientWiseMetrics.putIfAbsent(namespace, leaderElectionServiceMetrics);
    }

    private static double getLeaderElectionRateForAllClients() {
        return KeyedStream.stream(clientWiseMetrics)
                .values()
                .mapToDouble(leaderElectionRateForClient ->
                        leaderElectionRateForClient.proposedLeadership().getFiveMinuteRate())
                .sum();
    }

    public static LeaderElectionHealthStatus leaderElectionRateHealthStatus() {
        log.info("Blah | leader election rate is - ", getFormatted());
        return getLeaderElectionRateForAllClients() <= MAX_ALLOWED_LAST_5_MINUTE_RATE
                ? LeaderElectionHealthStatus.HEALTHY : LeaderElectionHealthStatus.UNHEALTHY;
    }

    private static String getFormatted() {
        StringBuilder sb = new StringBuilder("[");
        KeyedStream.stream(clientWiseMetrics).forEach((k, v) -> sb.append("(").append(k).append(" : ")
                .append(v.proposedLeadership().getFiveMinuteRate()).append(")\n"));
        sb.append("]");
        return sb.toString();
    }
}
