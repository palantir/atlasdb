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
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.factory.Leaders.LeaderUuidSupplier;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponses;
import com.palantir.timelock.TimelockStatus;
import com.palantir.timelock.TimelockStatus2s;

public class LeaderPingHealthCheck {

    private static final Duration HEALTH_CHECK_TIME_LIMIT = Duration.ofSeconds(7);

    private final Set<LeaderUuidSupplier> networkClients;
    private final ExecutorService executorService;

    public LeaderPingHealthCheck(Set<LeaderUuidSupplier> uuidSuppliers) {
        this.networkClients = uuidSuppliers;
        this.executorService = PTExecutors.newFixedThreadPool(
                uuidSuppliers.size(),
                new NamedThreadFactory("leader-ping-healthcheck", true));
    }

    public TimelockStatus getStatus() {
        PaxosResponses<PaxosContainer<UUID>> responses = PaxosQuorumChecker.collectAsManyResponsesAsPossible(
                ImmutableList.copyOf(networkClients),
                leaderUuid -> PaxosContainer.of(leaderUuid.get()),
                executorService,
                HEALTH_CHECK_TIME_LIMIT);

        int successes = responses.successes();
        if (successes < getQuorumSize()) {
            return TimelockStatus2s.quorumUnavailable(successes, networkClients.size());
        } else if (successes == getQuorumSize()) {
            return TimelockStatus2s.quorumAvailableDegradedState(successes, networkClients.size());
        } else {
            return TimelockStatus2s.quorumAvailableFaultTolerant();
        }
    }

    private int getQuorumSize() {
        return PaxosRemotingUtils.getQuorumSize(networkClients);
    }

}
