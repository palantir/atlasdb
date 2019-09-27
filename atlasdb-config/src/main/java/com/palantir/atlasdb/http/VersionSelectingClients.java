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

package com.palantir.atlasdb.http;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

/**
 * Factory for randomly selecting from a new and a legacy version of a client, based on a live-reloading probability.
 *
 * Please note that your clients will run independently; if there are stateful invariants that need to be enforced
 * across individual clients, you may need to share state appropriately.
 */
final class VersionSelectingClients {
    private static final String CLIENT_VERSION = "clientVersion";

    private VersionSelectingClients() {
        // No, nein, etc.
    }

    static <T> T createVersionSelectingClient(
            TaggedMetricRegistry taggedMetricRegistry,
            TargetFactory.InstanceAndVersion<T> newClient,
            TargetFactory.InstanceAndVersion<T> legacyClient,
            DoubleSupplier newClientProbabilitySupplier,
            Class<T> clazz) {
        T instrumentedNewClient = instrumentWithClientVersionTag(
                taggedMetricRegistry, newClient, clazz);
        T instrumentedLegacyClient = instrumentWithClientVersionTag(
                taggedMetricRegistry, legacyClient, clazz);

        return PredicateSwitchedProxy.newProxyInstance(
                instrumentedNewClient,
                instrumentedLegacyClient,
                () -> ThreadLocalRandom.current().nextDouble() < newClientProbabilitySupplier.getAsDouble(),
                clazz);
    }

    private static <T> T instrumentWithClientVersionTag(
            TaggedMetricRegistry taggedMetricRegistry,
            TargetFactory.InstanceAndVersion<T> client,
            Class<T> clazz) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                taggedMetricRegistry,
                clazz,
                client.instance(),
                MetricRegistry.name(clazz),
                $ -> ImmutableMap.of(CLIENT_VERSION, client.version()));
    }

}
