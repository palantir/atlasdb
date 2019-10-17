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

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.leader.AsyncLeadershipObserver;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.PingableLeader;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;

@Value.Immutable
public abstract class TimelockLeadershipMetrics {

    @Value.Parameter
    abstract TimelockPaxosMetrics metrics();

    @Value.Parameter
    abstract PingableLeader localPingableLeader();

    @Value.Derived
    MetricsManager metricsManager() {
        // we don't use the normal metric registry so we don't care about this
        return MetricsManagers.of(new MetricRegistry(), metrics().metrics());
    }

    public <T> T instrument(Client client, String name, Class<T> clazz, T instance) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                metrics().metrics(),
                clazz,
                instance,
                name,
                _context -> ImmutableMap.of(
                        AtlasDbMetricNames.TAG_CLIENT, client.value(),
                        AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER, String.valueOf(localPingableLeader().ping())));
    }

    @Value.Derived
    public LeadershipObserver leadershipObserver() {
        return AsyncLeadershipObserver.create(
                () -> metricsManager().deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(false)),
                () -> metricsManager().deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(true)));
    }

    @Value.Derived
    public List<SafeArg<Object>> namespaceAsArgs() {
        return ImmutableList.of(SafeArg.of(AtlasDbMetricNames.TAG_PAXOS_USE_CASE, metrics().paxosUseCase().toString()));
    }

    private static Predicate<MetricName> withTagIsCurrentSuspectedLeader(boolean currentLeader) {
        return metricName ->
                Optional.ofNullable(metricName.safeTags().get(AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER))
                        .filter(x -> x.equals(String.valueOf(currentLeader)))
                        .isPresent();
    }
}
