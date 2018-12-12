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
import com.palantir.logsafe.SafeArg;

public final class MetadataCoordinationServiceMetrics {
    private static final Logger log = LoggerFactory.getLogger(MetadataCoordinationServiceMetrics.class);

    private static final int DEFAULT_CACHE_TTL = 10;

    @VisibleForTesting
    static final String LAST_VALID_BOUND = "lastValidBound";
    @VisibleForTesting
    static final String EVENTUAL_TRANSACTIONS_SCHEMA_VERSION = "eventualTransactionsSchemaVersion";

    private MetadataCoordinationServiceMetrics() {
        // utility class
    }

    /**
     * Registers metrics that may be useful for diagnosing the status of a {@link CoordinationService}.
     *
     * Only one coordination service should be registered to a single metrics manager - if multiple are registered,
     * then it is non-deterministic as to which coordination service the metrics being reported are referring to.
     *
     * @param metricsManager metrics manager to register the
     * @param metadataCoordinationService metadata coordination service that should be tracked
     */
    public static void registerMetrics(
            MetricsManager metricsManager,
            CoordinationService<InternalSchemaMetadata> metadataCoordinationService) {
        registerValidityBoundMetric(metricsManager, metadataCoordinationService);
        registerEventualTransactionsSchemaVersionMetric(metricsManager, metadataCoordinationService);
    }

    /**
     * Registers a gauge which tracks the highest seen validity bound up to this point.
     *
     * @param metricsManager metrics manager to register the gauge on
     * @param metadataCoordinationService metadata coordination service that should be tracked
     */
    private static void registerValidityBoundMetric(MetricsManager metricsManager,
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
    }

    /**
     * Registers a gauge which tracks the eventual transactions schema version - that is, at the end of the current
     * period of validity for the bound, what the metadata says the transactions schema version should be.
     *
     * @param metricsManager metrics manager to register the gauge on
     * @param metadataCoordinationService metadata coordination service that should be tracked
     */
    private static void registerEventualTransactionsSchemaVersionMetric(MetricsManager metricsManager,
            CoordinationService<InternalSchemaMetadata> metadataCoordinationService) {
        metricsManager.registerMetric(
                MetadataCoordinationServiceMetrics.class,
                EVENTUAL_TRANSACTIONS_SCHEMA_VERSION,
                new CachedGauge<Integer>(Clock.defaultClock(), DEFAULT_CACHE_TTL, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        try {
                            Optional<ValueAndBound<InternalSchemaMetadata>> latestValue
                                    = metadataCoordinationService.getLastKnownLocalValue();
                            return latestValue
                                    .map(ValueAndBound::value)
                                    .flatMap(Function.identity())
                                    .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                                    .map(timestampMap -> timestampMap.getValueForTimestamp(latestValue.get().bound()))
                                    .orElse(null);
                        } catch (Exception e) {
                            log.info("An exception occurred when trying to retrieve the {} metric.",
                                    SafeArg.of("gaugeName", EVENTUAL_TRANSACTIONS_SCHEMA_VERSION),
                                    e);
                            return null;
                        }
                    }
                }
        );
    }

}
