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
package com.palantir.timelock.paxos;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;
import com.palantir.timelock.TimeLockStatus;

public class LeaderPingHealthCheck {

    private static final Duration HEALTH_CHECK_TIME_LIMIT = Duration.ofSeconds(7);

    private final Set<PingableLeader> leaders;
    private final ExecutorService executorService;

    public LeaderPingHealthCheck(Set<PingableLeader> leaders) {
        this.leaders = leaders;
        this.executorService = PTExecutors.newFixedThreadPool(
                leaders.size(),
                new NamedThreadFactory("leader-ping-healthcheck", true));
    }

    public TimeLockStatus getStatus() {
        PaxosResponses<HealthCheckResponse> responses = PaxosQuorumChecker.collectAsManyResponsesAsPossible(
                ImmutableList.copyOf(leaders),
                pingable -> new HealthCheckResponse(pingable.ping()),
                executorService,
                HEALTH_CHECK_TIME_LIMIT);

        long numLeaders = responses.stream()
                .filter(response -> response.isLeader)
                .count();

        if (responses.numberOfResponses() < getQuorumSize()) {
            return TimeLockStatus.NO_QUORUM;
        }

        if (numLeaders == 1) {
            return TimeLockStatus.ONE_LEADER;
        } else if (numLeaders == 0) {
            return TimeLockStatus.NO_LEADER;
        } else {
            return TimeLockStatus.MULTIPLE_LEADERS;
        }
    }

    private int getQuorumSize() {
        return PaxosRemotingUtils.getQuorumSize(leaders);
    }

    private static final class HealthCheckResponse implements PaxosResponse {

        private final boolean isLeader;

        private HealthCheckResponse(boolean isLeader) {
            this.isLeader = isLeader;
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }
    }

}
