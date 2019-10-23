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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.SlidingWindowTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class TimelockPaxosMetrics {

    abstract PaxosUseCase paxosUseCase();

    @Value.Derived
    TaggedMetricRegistry metrics() {
        return new SlidingWindowTaggedMetricRegistry(35, TimeUnit.SECONDS);
    }

    @Value.Derived
    MetricsManager asMetricsManager() {
        // we don't use the normal metric registry so we don't care about this
        return MetricsManagers.of(new MetricRegistry(), metrics());
    }

    public static TimelockPaxosMetrics of(PaxosUseCase paxosUseCase, TaggedMetricRegistry parentRegistry) {
        TimelockPaxosMetrics metrics = ImmutableTimelockPaxosMetrics.builder().paxosUseCase(paxosUseCase).build();
        metrics.attachToParentMetricRegistry(parentRegistry);
        return metrics;
    }

    public <T, U extends T> T instrument(Class<T> clazz, U instance, String name) {
        Map<String, String> tags = ImmutableMap.of();
        return AtlasDbMetrics.instrumentWithTaggedMetrics(metrics(), clazz, instance, name, _context -> tags);
    }

    public <T, U extends T> T instrument(Class<T> clazz, U instance, String name, Client client) {
        Map<String, String> tags = ImmutableMap.of(AtlasDbMetricNames.TAG_CLIENT, client.value());
        return AtlasDbMetrics.instrumentWithTaggedMetrics(metrics(), clazz, instance, name, _context -> tags);
    }

    public <T> LocalAndRemotes<T> instrumentLocalAndRemotesFor(Class<T> clazz, T local, List<T> remotes, String name) {
        return LocalAndRemotes.of(local, remotes)
                .map(instance -> instrument(clazz, instance, name));
    }

    public <T> LocalAndRemotes<T> instrumentLocalAndRemotesFor(
            Class<T> clazz,
            T local,
            List<T> remotes,
            String name,
            Client client) {
        return LocalAndRemotes.of(local, remotes)
                .map(instance -> instrument(clazz, instance, name, client));
    }

    private void attachToParentMetricRegistry(TaggedMetricRegistry parent) {
        parent.addMetrics(AtlasDbMetricNames.TAG_PAXOS_USE_CASE, paxosUseCase().toString(), metrics());
    }

}
