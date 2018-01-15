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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.management.InstanceNotFoundException;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.CassandraHealthMetric;
import com.palantir.atlasdb.qos.config.CassandraHealthMetricMeasurement;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetric;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetricMeasurement;
import com.palantir.cassandra.sidecar.metrics.CassandraMetricsService;

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

    private DeterministicScheduler scheduledExecutorService;
    private Supplier<List<CassandraHealthMetric>> healthMetricSupplier;
    private CassandraMetricsService cassandraMetricClient;
    private CassandraMetricMeasurementsLoader cassandraMetricMeasurementsLoader;

    @Before
    public void setup() {
        healthMetricSupplier = mock(Supplier.class);
        cassandraMetricClient = mock(CassandraMetricsService.class);
        scheduledExecutorService = new DeterministicScheduler();

        cassandraMetricMeasurementsLoader = new CassandraMetricMeasurementsLoader(
                healthMetricSupplier, cassandraMetricClient, scheduledExecutorService);

        when(healthMetricSupplier.get()).thenReturn(ImmutableList.of(
                getCassandraMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1),
                getCassandraMetric(METRIC_TYPE_2, METRIC_NAME_2, METRIC_ATTRIBUTE_2)));

        when(cassandraMetricClient.getMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1, ImmutableMap.of()))
                .thenReturn(METRIC_VALUE_1);

        when(cassandraMetricClient.getMetric(METRIC_TYPE_2, METRIC_NAME_2, METRIC_ATTRIBUTE_2, ImmutableMap.of()))
                .thenReturn(METRIC_VALUE_2);
    }

    @Test
    public void metricsLoaderLoadsMetricsEveryFiveSeconds() throws Exception {
        scheduledExecutorService.tick(1, TimeUnit.SECONDS);
        verify(healthMetricSupplier, times(1)).get();

        scheduledExecutorService.tick(4, TimeUnit.SECONDS);
        verify(healthMetricSupplier, times(2)).get();

        scheduledExecutorService.tick(5, TimeUnit.SECONDS);
        verify(healthMetricSupplier, times(3)).get();

        scheduledExecutorService.tick(5, TimeUnit.SECONDS);
        verify(healthMetricSupplier, times(4)).get();
    }

    @Test
    public void canReturnTheCassandraMetricsFromTheCassandraMetricService() throws Exception {
        scheduledExecutorService.tick(3, TimeUnit.SECONDS);

        List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                cassandraMetricMeasurementsLoader.getCassandraHealthMetricMeasurements();

        List<CassandraHealthMetricMeasurement> expectedCassandraHealthMetricMeasurements =
                ImmutableList.of(getCassandraMetricMeasurement(METRIC_VALUE_1),
                        getCassandraMetricMeasurement(METRIC_VALUE_2));

        assertThat(expectedCassandraHealthMetricMeasurements).isEqualTo(cassandraHealthMetricMeasurements);
    }

    @Test
    public void canHandleNonExistingMetric() throws Exception {
        assertThatMetricsReturnedAreEmptyIfCassandraMetricsServiceThrows(InstanceNotFoundException.class);
    }

    @Test
    public void canhandleFailureGettingMetric() throws Exception {
        assertThatMetricsReturnedAreEmptyIfCassandraMetricsServiceThrows(ConnectException.class);
    }

    private void assertThatMetricsReturnedAreEmptyIfCassandraMetricsServiceThrows(Class clazz) {
        when(cassandraMetricClient.getMetric(METRIC_TYPE_1, METRIC_NAME_1, METRIC_ATTRIBUTE_1, ImmutableMap.of()))
                .thenThrow(clazz);

        scheduledExecutorService.tick(3, TimeUnit.SECONDS);

        List<CassandraHealthMetricMeasurement> cassandraHealthMetricMeasurements =
                cassandraMetricMeasurementsLoader.getCassandraHealthMetricMeasurements();

        assertThat(ImmutableList.of()).isEqualTo(cassandraHealthMetricMeasurements);
    }

    private CassandraHealthMetric getCassandraMetric(String metricType, String metricName,
            String metricAttribute) {
        return ImmutableCassandraHealthMetric.builder()
                .type(metricType)
                .name(metricName)
                .attribute(metricAttribute)
                .additionalParams(ImmutableMap.of())
                .lowerLimit(LOWER_LIMIT)
                .upperLimit(UPPER_LIMIT)
                .build();
    }

    private CassandraHealthMetricMeasurement getCassandraMetricMeasurement(Double currentValue) {
        return ImmutableCassandraHealthMetricMeasurement.builder()
                .currentValue(currentValue)
                .lowerLimit(LOWER_LIMIT)
                .upperLimit(UPPER_LIMIT)
                .build();
    }
}
