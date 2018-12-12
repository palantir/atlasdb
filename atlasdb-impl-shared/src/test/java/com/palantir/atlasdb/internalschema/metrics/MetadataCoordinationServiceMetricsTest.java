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
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

@SuppressWarnings("unchecked")
public class MetadataCoordinationServiceMetricsTest {
    private static final String LAST_VALID_BOUND = MetadataCoordinationServiceMetrics.LAST_VALID_BOUND;
    private static final long TIMESTAMP_1 = 1111L;
    private static final long TIMESTAMP_2 = 2222L;

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final CoordinationService<InternalSchemaMetadata> metadataCoordinationService
            = mock(CoordinationService.class);

    @Before
    public void setUp() {
        when(metadataCoordinationService.getLastKnownLocalValue())
                .thenReturn(Optional.of(ValueAndBound.of(Optional.empty(), TIMESTAMP_1)));
        MetadataCoordinationServiceMetrics.registerMetrics(metricsManager, metadataCoordinationService);
    }

    @Test
    public void returnsValidityBoundFromCoordinationService() {
        Gauge<Long> boundGauge = getGauge(metricsManager, LAST_VALID_BOUND);
        assertThat(boundGauge.getValue()).isEqualTo(TIMESTAMP_1);
        verify(metadataCoordinationService).getLastKnownLocalValue();
    }

    @Test
    public void handlesMultitenancyCorrectly() {
        CoordinationService<InternalSchemaMetadata> otherService = mock(CoordinationService.class);
        MetricsManager otherManager = MetricsManagers.createForTests();
        MetadataCoordinationServiceMetrics.registerMetrics(otherManager, otherService);

        when(otherService.getLastKnownLocalValue()).thenReturn(
                Optional.of(ValueAndBound.of(Optional.empty(), TIMESTAMP_2)));

        assertThat(getGauge(metricsManager, LAST_VALID_BOUND).getValue()).isEqualTo(TIMESTAMP_1);
        assertThat(getGauge(otherManager, LAST_VALID_BOUND).getValue()).isEqualTo(TIMESTAMP_2);
    }

    private static String buildFullyQualifiedMetricName(String shortName) {
        return MetricRegistry.name(MetadataCoordinationServiceMetrics.class, shortName);
    }

    @SuppressWarnings("unchecked") // We know the gauges we are registering produce Longs
    private Gauge<Long> getGauge(MetricsManager manager, String shortName) {
        return (Gauge<Long>) manager.getRegistry()
                .getGauges()
                .get(buildFullyQualifiedMetricName(shortName));
    }
}