/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.timelock.paxos.AutobatchingLeadershipObserverFactory.LeadershipEvent;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.PaxosLeadershipEventRecorder;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.paxos.Client;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.immutables.value.Value;

@Value.Immutable
public abstract class TimelockLeadershipMetrics implements Dependencies.LeadershipMetrics {

    @Value.Derived
    List<SafeArg<String>> namespaceAsLoggingArgs() {
        return ImmutableList.of(
                SafeArg.of(AtlasDbMetricNames.TAG_PAXOS_USE_CASE, metrics().paxosUseCase().toString()),
                SafeArg.of(AtlasDbMetricNames.TAG_CLIENT, proxyClient().value()));
    }

    @Value.Derived
    public PaxosLeadershipEventRecorder eventRecorder() {
        return PaxosLeadershipEventRecorder.create(
                metrics().clientScopedMetrics().metricRegistryForClient(proxyClient()), // metrics for client etc.
                leaderUuid().toString(),
                leadershipObserver(),
                namespaceAsLoggingArgs());
    }

    @Value.Derived
    LeadershipObserver leadershipObserver() {
        return leadershipObserverFactory().create(proxyClient());
    }

    public <T> T instrument(Class<T> clazz, T instance) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                metrics().clientScopedMetrics().metricRegistryForClient(proxyClient()),
                clazz,
                instance,
                _context -> ImmutableMap.of(
                        AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER, String.valueOf(localPingableLeader().ping())));
    }

    public static AutobatchingLeadershipObserverFactory createFactory(TimelockPaxosMetrics metrics) {
        return AutobatchingLeadershipObserverFactory.create(getMetricDeregistrator(metrics));
    }

    private static Consumer<SetMultimap<LeadershipEvent, Client>> getMetricDeregistrator(TimelockPaxosMetrics metrics) {
        PaxosUseCase paxosUseCase = metrics.paxosUseCase();
        switch (paxosUseCase) {
            case LEADER_FOR_ALL_CLIENTS:
                return deregisterMetricsForSingleLeader(metrics);
            case LEADER_FOR_EACH_CLIENT:
                return deregisterMetricsForPartitionedLeader(metrics);
            case TIMESTAMP:
                throw new SafeIllegalArgumentException("Timestamp paxos not supported");
            default:
                throw new SafeIllegalArgumentException("Unexpected paxosUseCase",  SafeArg.of("useCase", paxosUseCase));
        }
    }

    private static Consumer<SetMultimap<LeadershipEvent, Client>> deregisterMetricsForSingleLeader(
            TimelockPaxosMetrics metrics) {
        return eventsToDeregister -> eventsToDeregister.keySet().forEach(event -> metrics.asMetricsManager()
                .deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(event.isCurrentSuspectedLeader())));
    }

    private static Consumer<SetMultimap<LeadershipEvent, Client>> deregisterMetricsForPartitionedLeader(
            TimelockPaxosMetrics metrics) {
        return eventsToDeregister ->
                eventsToDeregister.forEach((event, client) -> {
                    metrics.clientScopedMetrics().deregisterMetric(client,
                            withTagIsCurrentSuspectedLeader(event.isCurrentSuspectedLeader()));
                });
    }

    private static Predicate<MetricName> withTagIsCurrentSuspectedLeader(boolean currentLeader) {
        return metricName ->
                Optional.ofNullable(metricName.safeTags().get(AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER))
                        .filter(String.valueOf(currentLeader)::equals)
                        .isPresent();
    }

}
