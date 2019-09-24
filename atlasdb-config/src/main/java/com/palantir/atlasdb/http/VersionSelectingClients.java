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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

class VersionSelectingClients {
    private static final String CLIENT_VERSION = "clientVersion";
    private static final Map<String, String> NEW_CLIENT_TAG = ImmutableMap.of(CLIENT_VERSION, "new");
    private static final Map<String, String> LEGACY_CLIENT_TAG = ImmutableMap.of(CLIENT_VERSION, "legacy");

    private VersionSelectingClients() {
        // No, nein, etc.
    }

    static <T> T createVersionSelectingClient(
            TaggedMetricRegistry taggedMetricRegistry,
            T uninstrumentedNewClient,
            T uninstrumentedLegacyClient,
            DoubleSupplier newClientProbabilitySupplier,
            Class<T> clazz) {
        T instrumentedNewClient = instrumentWithClientVersionTag(
                taggedMetricRegistry, uninstrumentedNewClient, clazz, NEW_CLIENT_TAG);
        T instrumentedLegacyClient = instrumentWithClientVersionTag(
                taggedMetricRegistry, uninstrumentedLegacyClient, clazz, LEGACY_CLIENT_TAG);

        return PredicateSwitchedProxy.newProxyInstance(
                instrumentedNewClient,
                instrumentedLegacyClient,
                () -> ThreadLocalRandom.current().nextDouble() < newClientProbabilitySupplier.getAsDouble(),
                clazz);
    }

    private static <T> T instrumentWithClientVersionTag(
            TaggedMetricRegistry taggedMetricRegistry,
            T uninstrumentedLegacyClient,
            Class<T> clazz,
            Map<String, String> tagToApply) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                taggedMetricRegistry,
                clazz,
                uninstrumentedLegacyClient,
                MetricRegistry.name(clazz),
                $ -> tagToApply);
    }
}
