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

package com.palantir.atlasdb.qos.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

public class SimpleThrottlingStrategyTest {
    @Test
    public void clientLimitMultiplierIsOneForNoMetrics() throws Exception {
        double clientLimitMultiplier = new SimpleThrottlingStrategy().getClientLimitMultiplier(ImmutableList::of);
        assertThat(clientLimitMultiplier).isEqualTo(1.0);
    }

    @Test
    public void clientLimitMultiplierIsIncreasingForCassandraHealthyIndicatingMetrics() throws Exception {
        SimpleThrottlingStrategy simpleThrottlingStrategy = new SimpleThrottlingStrategy();

        Supplier<Collection<? extends CassandraHealthMetricMeasurement>> metricMeasurements = () -> ImmutableList.of(
                getMetricMeasurement(5, 10, 8), getMetricMeasurement(4, 8, 6));

        assertThat(simpleThrottlingStrategy.getClientLimitMultiplier(metricMeasurements)).isEqualTo(1.0);
    }

    @Test
    public void clientLimitMultiplierIsDecreasingForOneCassandraUnhealthyIndicatingMetrics() throws Exception {
        SimpleThrottlingStrategy simpleThrottlingStrategy = new SimpleThrottlingStrategy();

        Supplier<Collection<? extends CassandraHealthMetricMeasurement>> metricMeasurements = () -> ImmutableList.of(
                getMetricMeasurement(5, 10, 15), getMetricMeasurement(4, 8, 6));

        assertThat(simpleThrottlingStrategy.getClientLimitMultiplier(metricMeasurements)).isEqualTo(0.5);
    }

    @Test
    public void clientLimitMultiplierIsDecreasingForAllCassandraUnhealthyIndicatingMetrics() throws Exception {
        SimpleThrottlingStrategy simpleThrottlingStrategy = new SimpleThrottlingStrategy();

        Supplier<Collection<? extends CassandraHealthMetricMeasurement>> metricMeasurements = () -> ImmutableList.of(
                getMetricMeasurement(5, 10, 15), getMetricMeasurement(4, 8, 2));

        assertThat(simpleThrottlingStrategy.getClientLimitMultiplier(metricMeasurements)).isEqualTo(0.5);
    }

    @Test
    public void clientLimitMultiplierIsDecreasingAndThenIncreasingForCassandraUnhealthyAndThenHealthyIndicatingMetrics()
            throws Exception {
        SimpleThrottlingStrategy simpleThrottlingStrategy = new SimpleThrottlingStrategy();

        Supplier<Collection<? extends CassandraHealthMetricMeasurement>> unhealthyMetricMeasurements = () ->
                ImmutableList.of(getMetricMeasurement(5, 10, 15), getMetricMeasurement(4, 8, 2));

        Supplier<Collection<? extends CassandraHealthMetricMeasurement>> healthyMetricMeasurements = () ->
                ImmutableList.of(getMetricMeasurement(5, 10, 8), getMetricMeasurement(4, 8, 6));

        assertThat(simpleThrottlingStrategy.getClientLimitMultiplier(unhealthyMetricMeasurements)).isEqualTo(0.5);
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        assertThat(simpleThrottlingStrategy.getClientLimitMultiplier(healthyMetricMeasurements)).isEqualTo(0.55);
    }

    private CassandraHealthMetricMeasurement getMetricMeasurement(double lowerLimit, double upperLimit,
            double currentVal) {
        return ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(lowerLimit)
                .upperLimit(upperLimit)
                .currentValue(currentVal)
                .build();
    }
}
