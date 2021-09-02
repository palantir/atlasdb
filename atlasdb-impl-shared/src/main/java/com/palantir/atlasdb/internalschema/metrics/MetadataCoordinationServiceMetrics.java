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

import com.codahale.metrics.Clock;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.monitoring.TrackerUtils;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampService;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public final class MetadataCoordinationServiceMetrics {
    private static final SafeLogger log = SafeLoggerFactory.get(MetadataCoordinationServiceMetrics.class);

    private MetadataCoordinationServiceMetrics() {
        // utility class
    }

    /**
     * Registers metrics that may be useful for diagnosing the status of a {@link CoordinationService}.
     * <p>
     * Only one coordination service should be registered to a single metrics manager - if multiple are registered, then
     * it is non-deterministic as to which coordination service the metrics being reported are referring to.
     *
     * @param metricsManager metrics manager to register the
     * @param metadataCoordinationService metadata coordination service that should be tracked
     * @param timestamp timestamp service that this coordination service uses for sequencing
     */
    public static void registerMetrics(
            MetricsManager metricsManager,
            CoordinationService<InternalSchemaMetadata> metadataCoordinationService,
            TimestampService timestamp) {
        registerValidityBoundMetric(metricsManager, metadataCoordinationService);
        registerTransactionsSchemaVersionMetrics(metricsManager, metadataCoordinationService, timestamp);
    }

    /**
     * Registers a gauge which tracks the current value of the validity bound. Under normal operation and within the
     * lifetime of a single JVM, this should not decrease.
     *
     * @param metricsManager metrics manager to register the gauge on
     * @param metadataCoordinationService metadata coordination service that should be tracked
     */
    private static void registerValidityBoundMetric(
            MetricsManager metricsManager, CoordinationService<InternalSchemaMetadata> metadataCoordinationService) {
        metricsManager.registerMetric(
                MetadataCoordinationServiceMetrics.class,
                AtlasDbMetricNames.COORDINATION_LAST_VALID_BOUND,
                TrackerUtils.createCachingExceptionHandlingGauge(
                        log,
                        Clock.defaultClock(),
                        AtlasDbMetricNames.COORDINATION_LAST_VALID_BOUND,
                        () -> metadataCoordinationService
                                .getLastKnownLocalValue()
                                .map(ValueAndBound::bound)
                                .orElse(Long.MIN_VALUE)));
    }

    /**
     * Registers two gauges which track transactions schema versions:
     *
     * 1. the eventual schema version - that is, at the end of the current
     * period of validity for the bound, what the metadata says the transactions schema version should be.
     * 2. the current schema version - that is, the schema version at a fresh timestamp.
     *
     * @param metricsManager metrics manager to register the gauge on
     * @param metadataCoordinationService metadata coordination service that should be tracked
     */
    // TODO (jkong): Cache the reads from the supplier, but/and handle exceptions in the gauges.
    private static void registerTransactionsSchemaVersionMetrics(
            MetricsManager metricsManager,
            CoordinationService<InternalSchemaMetadata> metadataCoordinationService,
            TimestampService timestampService) {
        Supplier<Optional<ValueAndBound<TimestampPartitioningMap<Integer>>>> valueAndBoundSupplier =
                () -> MetadataCoordinationServiceMetrics.getTimestampToTransactionsTableSchemaVersionMap(
                        metadataCoordinationService);
        registerMetricForTransactionsSchemaVersionAtTimestamp(
                metricsManager,
                AtlasDbMetricNames.COORDINATION_EVENTUAL_TRANSACTIONS_SCHEMA_VERSION,
                valueAndBoundSupplier,
                ValueAndBound::bound);
        registerMetricForTransactionsSchemaVersionAtTimestamp(
                metricsManager,
                AtlasDbMetricNames.COORDINATION_CURRENT_TRANSACTIONS_SCHEMA_VERSION,
                valueAndBoundSupplier,
                unused -> timestampService.getFreshTimestamp());
    }

    private static Optional<ValueAndBound<TimestampPartitioningMap<Integer>>>
            getTimestampToTransactionsTableSchemaVersionMap(
                    CoordinationService<InternalSchemaMetadata> metadataCoordinationService) {
        Optional<ValueAndBound<InternalSchemaMetadata>> latestValue =
                metadataCoordinationService.getLastKnownLocalValue();
        return latestValue
                .map(ValueAndBound::value)
                .flatMap(Function.identity())
                .map(InternalSchemaMetadata::timestampToTransactionsTableSchemaVersion)
                .map(partitioningMap ->
                        ValueAndBound.of(partitioningMap, latestValue.get().bound()));
    }

    private static void registerMetricForTransactionsSchemaVersionAtTimestamp(
            MetricsManager metricsManager,
            String metricName,
            Supplier<Optional<ValueAndBound<TimestampPartitioningMap<Integer>>>> timestampMapSupplier,
            Function<ValueAndBound<TimestampPartitioningMap<Integer>>, Long> timestampQuery) {
        metricsManager.registerMetric(
                MetadataCoordinationServiceMetrics.class,
                metricName,
                TrackerUtils.createCachingExceptionHandlingGauge(
                        log, Clock.defaultClock(), metricName, () -> timestampMapSupplier
                                .get()
                                .map(mapAndBound ->
                                        getVersionAtTimestamp(mapAndBound.value(), timestampQuery.apply(mapAndBound)))
                                .orElse(null)));
    }

    private static Integer getVersionAtTimestamp(Optional<TimestampPartitioningMap<Integer>> timestampMap, long bound) {
        return timestampMap.map(map -> map.getValueForTimestamp(bound)).orElse(null);
    }
}
