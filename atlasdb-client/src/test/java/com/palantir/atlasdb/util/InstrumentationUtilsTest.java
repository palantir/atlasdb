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

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.tritium.metrics.registry.MetricName;
import java.util.function.LongSupplier;
import org.junit.Test;

public class InstrumentationUtilsTest {
    private static final RuntimeException EXCEPTION = new RuntimeException();
    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final LongSupplier mockSupplier = mock(LongSupplier.class);

    @Test
    public void failureMetricsForTaggedAndUntaggedElementsAreDistinct() {
        when(mockSupplier.getAsLong()).thenThrow(EXCEPTION);
        LongSupplier taggedSupplier = AtlasDbMetrics.instrumentWithTaggedMetrics(
                metricsManager.getTaggedRegistry(), LongSupplier.class, mockSupplier);
        LongSupplier legacySupplier =
                AtlasDbMetrics.instrument(metricsManager.getRegistry(), LongSupplier.class, mockSupplier);

        assertThatThrownBy(taggedSupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(taggedSupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(taggedSupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(taggedSupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(legacySupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(legacySupplier::getAsLong).isEqualTo(EXCEPTION);
        assertThatThrownBy(legacySupplier::getAsLong).isEqualTo(EXCEPTION);

        assertThat(metricsManager
                        .getTaggedRegistry()
                        .meter(InstrumentationUtils.TAGGED_FAILURES_METRIC_NAME)
                        .getCount())
                .isEqualTo(4);
        assertThat(metricsManager
                        .getRegistry()
                        .meter(InstrumentationUtils.TAGGED_FAILURES_METRIC_NAME.safeName())
                        .getCount())
                .isEqualTo(3);
    }

    @Test
    public void untaggedAndTaggedMetricRegistryFailureMetricsHaveDifferentNames() {
        MetricName legacyFailureMetricName = MetricName.builder()
                .safeName(InstrumentationUtils.TAGGED_FAILURES_METRIC_NAME.safeName())
                .build();
        assertThat(InstrumentationUtils.TAGGED_FAILURES_METRIC_NAME).isNotEqualTo(legacyFailureMetricName);
    }
}
