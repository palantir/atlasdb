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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.LeaderElectionServiceMetrics;

public class LeaderElectionHealthCheck {
    private static final Logger log = LoggerFactory.getLogger(LeaderElectionHealthCheck.class);
    private static final double MAX_ALLOWED_LAST_5_MINUTE_RATE = 0.0095;

    private final ConcurrentMap<String, LeaderElectionServiceMetrics> clientWiseMetrics = new ConcurrentHashMap<>();


    public LeaderElectionServiceMetrics registerClient(String namespace,
            LeaderElectionServiceMetrics leaderElectionServiceMetrics) {
        return clientWiseMetrics.putIfAbsent(namespace, leaderElectionServiceMetrics);
    }

    private Stream<Map.Entry<? extends String, ? extends LeaderElectionServiceMetrics>> getOffenders() {
        return KeyedStream.stream(clientWiseMetrics)
                .filter(val -> val.proposedLeadership().getFiveMinuteRate() <= MAX_ALLOWED_LAST_5_MINUTE_RATE).entries();
    }

    public LeaderElectionHealthStatus leaderElectionRateHealthStatus() {
        log.info("leader election rate is - ", getOffenders());
        return LeaderElectionHealthStatus.HEALTHY;
    }
}
