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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponses;
import com.palantir.timelock.TimeLockStatus;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class LeaderPingHealthCheck {

    private static final Duration HEALTH_CHECK_TIME_LIMIT = Duration.ofSeconds(7);

    private final NamespaceTracker namespaceTracker;
    private final List<HealthCheckPinger> pingers;
    private final ExecutorService executorService;

    public LeaderPingHealthCheck(NamespaceTracker namespaceTracker, List<HealthCheckPinger> pingers) {
        this.namespaceTracker = namespaceTracker;
        this.pingers = pingers;
        this.executorService = PTExecutors.newFixedThreadPool(pingers.size(), "leader-ping-healthcheck");
    }

    public HealthCheckDigest getStatus() {
        Set<Client> namespacesToCheck = namespaceTracker.trackedNamespaces();

        PaxosResponses<PaxosContainer<Map<Client, HealthCheckResponse>>> responses =
                PaxosQuorumChecker.collectAsManyResponsesAsPossible(
                        ImmutableList.copyOf(pingers),
                        pinger -> PaxosContainer.of(pinger.apply(namespacesToCheck)),
                        executorService,
                        HEALTH_CHECK_TIME_LIMIT,
                        PaxosConstants.CANCEL_REMAINING_CALLS);

        Map<Client, PaxosResponses<HealthCheckResponse>> responsesByClient = responses.stream()
                .map(PaxosContainer::get)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.mapping(
                                Map.Entry::getValue,
                                Collectors.collectingAndThen(
                                        Collectors.toList(), r -> PaxosResponses.of(getQuorumSize(), r)))));

        SetMultimap<TimeLockStatus, Client> statusesToClient = KeyedStream.stream(responsesByClient)
                .map(this::convertQuorumResponsesToStatus)
                .mapEntries((client, status) -> Maps.immutableEntry(status, client))
                .collectToSetMultimap();

        return ImmutableHealthCheckDigest.of(statusesToClient);
    }

    private int getQuorumSize() {
        return PaxosRemotingUtils.getQuorumSize(pingers);
    }

    private TimeLockStatus convertQuorumResponsesToStatus(PaxosResponses<HealthCheckResponse> responses) {
        long numLeaders =
                responses.stream().filter(HealthCheckResponse::isLeader).count();

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
}
