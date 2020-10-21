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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timestamp.TimestampService;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mocks
public class MetadataCoordinationServiceMetricsTest {
    private static final long TIMESTAMP_1 = 1111L;
    private static final long TIMESTAMP_2 = 2222L;
    private static final InternalSchemaMetadata INTERNAL_SCHEMA_METADATA = InternalSchemaMetadata.builder()
            .timestampToTransactionsTableSchemaVersion(
                    TimestampPartitioningMap.of(ImmutableRangeMap.<Long, Integer>builder()
                            .put(Range.range(1L, BoundType.CLOSED, TIMESTAMP_1, BoundType.OPEN), 1)
                            .put(Range.atLeast(TIMESTAMP_1), 2)
                            .build()))
            .build();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final CoordinationService<InternalSchemaMetadata> metadataCoordinationService =
            mock(CoordinationService.class);
    private final TimestampService timestampService = mock(TimestampService.class);

    @Before
    public void setUp() {
        when(metadataCoordinationService.getLastKnownLocalValue())
                .thenReturn(Optional.of(ValueAndBound.of(INTERNAL_SCHEMA_METADATA, TIMESTAMP_1)));
        MetadataCoordinationServiceMetrics.registerMetrics(
                metricsManager, metadataCoordinationService, timestampService);
    }

    @Test
    public void returnsValidityBoundFromCoordinationService() {
        Gauge<Long> boundGauge = getGauge(metricsManager, AtlasDbMetricNames.COORDINATION_LAST_VALID_BOUND);
        assertThat(boundGauge.getValue()).isEqualTo(TIMESTAMP_1);
        verify(metadataCoordinationService).getLastKnownLocalValue();
        verifyNoMoreInteractions(metadataCoordinationService);
    }

    @Test
    public void returnsEventualTransactionsSchemaVersionFromCoordinationService() {
        Gauge<Integer> boundGauge =
                getGauge(metricsManager, AtlasDbMetricNames.COORDINATION_EVENTUAL_TRANSACTIONS_SCHEMA_VERSION);
        assertThat(boundGauge.getValue()).isEqualTo(2);
        verify(metadataCoordinationService).getLastKnownLocalValue();
        verifyNoMoreInteractions(metadataCoordinationService);
    }

    @Test
    public void returnsOldTransactionsSchemaVersionAsCurrentIfNewVersionNotActiveYet() {
        Gauge<Integer> boundGauge =
                getGauge(metricsManager, AtlasDbMetricNames.COORDINATION_CURRENT_TRANSACTIONS_SCHEMA_VERSION);
        when(timestampService.getFreshTimestamp()).thenReturn(TIMESTAMP_1 - 1);
        assertThat(boundGauge.getValue()).isEqualTo(1);
        verify(metadataCoordinationService).getLastKnownLocalValue();
        verifyNoMoreInteractions(metadataCoordinationService);
    }

    @Test
    public void returnsNewTransactionsSchemaVersionAsCurrentIfNewVersionHasTakenEffect() {
        Gauge<Integer> boundGauge =
                getGauge(metricsManager, AtlasDbMetricNames.COORDINATION_CURRENT_TRANSACTIONS_SCHEMA_VERSION);
        when(timestampService.getFreshTimestamp()).thenReturn(TIMESTAMP_1);
        assertThat(boundGauge.getValue()).isEqualTo(2);
        verify(metadataCoordinationService).getLastKnownLocalValue();
        verifyNoMoreInteractions(metadataCoordinationService);
    }

    @Test
    public void handlesMultitenancyCorrectly() {
        CoordinationService<InternalSchemaMetadata> otherService = mock(CoordinationService.class);
        MetricsManager otherManager = MetricsManagers.createForTests();
        TimestampService otherTimestampService = mock(TimestampService.class);
        MetadataCoordinationServiceMetrics.registerMetrics(otherManager, otherService, otherTimestampService);

        when(otherService.getLastKnownLocalValue())
                .thenReturn(Optional.of(ValueAndBound.of(Optional.empty(), TIMESTAMP_2)));

        assertThat(getGauge(metricsManager, AtlasDbMetricNames.COORDINATION_LAST_VALID_BOUND)
                        .getValue())
                .isEqualTo(TIMESTAMP_1);
        assertThat(getGauge(otherManager, AtlasDbMetricNames.COORDINATION_LAST_VALID_BOUND)
                        .getValue())
                .isEqualTo(TIMESTAMP_2);
    }

    private static String buildFullyQualifiedMetricName(String shortName) {
        return MetricRegistry.name(MetadataCoordinationServiceMetrics.class, shortName);
    }

    private static <T> Gauge<T> getGauge(MetricsManager manager, String shortName) {
        return manager.getRegistry().getGauges().get(buildFullyQualifiedMetricName(shortName));
    }
}
