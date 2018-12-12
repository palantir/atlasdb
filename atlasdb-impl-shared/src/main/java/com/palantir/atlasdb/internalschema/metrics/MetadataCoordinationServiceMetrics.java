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

package com.palantir.atlasdb.internalschema.metrics;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.monitoring.TrackerUtils;
import com.palantir.atlasdb.util.MetricsManager;

public class MetadataCoordinationServiceMetrics {
    private static final Logger log = LoggerFactory.getLogger(MetadataCoordinationServiceMetrics.class);

    @VisibleForTesting
    static final String LAST_VALID_BOUND = "lastValidBound";

    @VisibleForTesting
    static final String EVENTUAL_TRANSACTIONS_SCHEMA_VERSION = "eventualTransactionsSchemaVersion";

    private MetadataCoordinationServiceMetrics() {
        // utility class
    }

    public static void registerMetrics(
            MetricsManager metricsManager,
            CoordinationService<InternalSchemaMetadata> metadataCoordinationService) {
        metricsManager.registerMetric(
                MetadataCoordinationServiceMetrics.class,
                LAST_VALID_BOUND,
                TrackerUtils.createCachingMonotonicIncreasingGauge(
                        log,
                        Clock.defaultClock(),
                        LAST_VALID_BOUND,
                        () -> metadataCoordinationService.getLastKnownLocalValue()
                                .map(ValueAndBound::bound)
                                .orElse(Long.MIN_VALUE)));
        metricsManager.registerMetric(
                MetadataCoordinationServiceMetrics.class,
                EVENTUAL_TRANSACTIONS_SCHEMA_VERSION,
                new CachedGauge<Integer>(Clock.defaultClock(), 10, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        Optional<ValueAndBound<InternalSchemaMetadata>> latestValue
                                = metadataCoordinationService.getLastKnownLocalValue();
                        return latestValue
                                .map(ValueAndBound::value)
                                .flatMap(Function.identity())
                                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                                .map(timestampMap -> timestampMap.getValueForTimestamp(latestValue.get().bound()))
                                .orElse(null);
                    }
                }
        );
    }
}
