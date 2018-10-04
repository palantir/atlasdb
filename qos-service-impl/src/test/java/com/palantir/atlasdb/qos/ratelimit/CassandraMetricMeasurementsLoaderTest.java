/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.qos.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.HealthMetric;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableHealthMetric;
import com.palantir.atlasdb.qos.metrics.MetricsService;

public class CassandraMetricMeasurementsLoaderTest {
    private static final double LOWER_LIMIT = 0.0;
    private static final double UPPER_LIMIT = 10.0;
    private static final double METRIC_VALUE_1 = 15.0;
    private static final double METRIC_VALUE_2 = 20.0;
    private static final String METRIC_TYPE_1 = "metricType1";
    private static final String METRIC_TYPE_2 = "metricType2";
    private static final String METRIC_NAME_1 = "metricName1";
    private static final String METRIC_NAME_2 = "metricName2";
    private static final String METRIC_ATTRIBUTE_1 = "metricAttribute1";
    private static final String METRIC_ATTRIBUTE_2 = "metricAttribute2";
    private static final Supplier<List<HealthMetric>> HEALTH_METRIC_SUPPLIER = () -> ImmutableList.of(
            getCassandraMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1),
            getCassandraMetric(METRIC_TYPE_2, METRIC_NAME_2, METRIC_ATTRIBUTE_2));

    private final DeterministicScheduler scheduledExecutorService = new DeterministicScheduler();
    private final MetricsService cassandraMetricClient = mock(MetricsService.class);
    private CassandraMetricMeasurementsLoader cassandraMetricMeasurementsLoader;

    @Before
    public void setup() {
        cassandraMetricMeasurementsLoader = new CassandraMetricMeasurementsLoader(
                HEALTH_METRIC_SUPPLIER, cassandraMetricClient, scheduledExecutorService);

        when(cassandraMetricClient.getMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1, ImmutableMap.of()))
                .thenReturn(METRIC_VALUE_1);

        when(cassandraMetricClient.getMetric(METRIC_TYPE_2, METRIC_NAME_2, METRIC_ATTRIBUTE_2, ImmutableMap.of()))
                .thenReturn(METRIC_VALUE_2);
    }

    @Test
    public void canReturnTheCassandraMetricsFromTheCassandraMetricService() {
        scheduledExecutorService.tick(3, TimeUnit.SECONDS);

        List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                cassandraMetricMeasurementsLoader.getCassandraHealthMetricMeasurements();

        List<CassandraHealthMetricMeasurement> expectedCassandraHealthMetricMeasurements =
                ImmutableList.of(getCassandraMetricMeasurement(METRIC_VALUE_1),
                        getCassandraMetricMeasurement(METRIC_VALUE_2));

        assertThat(expectedCassandraHealthMetricMeasurements).isEqualTo(cassandraHealthMetricMeasurements);
    }

    @Test
    public void canHandleFailureWhenFetchingMetric() {
        when(cassandraMetricClient.getMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1, ImmutableMap.of()))
                .thenThrow(new RuntimeException("something went wrong"));

        scheduledExecutorService.tick(10, TimeUnit.SECONDS);

        assertThat(cassandraMetricMeasurementsLoader.getCassandraHealthMetricMeasurements()).isEmpty();
    }

    private static HealthMetric getCassandraMetric(String metricType, String metricName, String metricAttribute) {
        return ImmutableHealthMetric.builder()
                .type(metricType)
                .name(metricName)
                .attribute(metricAttribute)
                .additionalParams(ImmutableMap.of())
                .lowerLimit(LOWER_LIMIT)
                .upperLimit(UPPER_LIMIT)
                .build();
    }

    private static CassandraHealthMetricMeasurement getCassandraMetricMeasurement(Double currentValue) {
        return ImmutableCassandraHealthMetricMeasurement.builder()
                .currentValue(currentValue)
                .lowerLimit(LOWER_LIMIT)
                .upperLimit(UPPER_LIMIT)
                .build();
    }
}
