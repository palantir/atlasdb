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

package com.palantir.atlasdb.timelock.paxos;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.paxos.Client;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SlidingWindowTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ClientScopedMetrics {

    private final TaggedMetricRegistry parentRegistry;
    private final Map<Client, MetricsManager> clientScopedMetricRegistry = new ConcurrentHashMap<>();

    ClientScopedMetrics(TaggedMetricRegistry parentRegistry) {
        this.parentRegistry = parentRegistry;
    }

    public TaggedMetricRegistry metricRegistryForClient(Client client) {
        return getOrCreateMetricsManager(client).getTaggedRegistry();
    }

    public void deregisterMetric(Client client, Predicate<MetricName> metricNamePredicate) {
        getOrCreateMetricsManager(client).deregisterTaggedMetrics(metricNamePredicate);
    }

    private MetricsManager getOrCreateMetricsManager(Client client) {
        return clientScopedMetricRegistry.computeIfAbsent(client, this::createScopedMetricsManager);
    }

    private MetricsManager createScopedMetricsManager(Client client) {
        TaggedMetricRegistry childRegistry = new SlidingWindowTaggedMetricRegistry(35, TimeUnit.SECONDS);
        parentRegistry.addMetrics(AtlasDbMetricNames.TAG_CLIENT, client.value(), childRegistry);
        return MetricsManagers.of(new MetricRegistry(), childRegistry);
    }
}
