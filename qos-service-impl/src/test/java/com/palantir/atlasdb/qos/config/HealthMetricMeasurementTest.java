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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HealthMetricMeasurementTest {
    @Test
    public void canCreateAValidMetricMeasurementWithValueHigherThanUpperLimit() {
        ImmutableCassandraHealthMetricMeasurement measurement = ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(20)
                .upperLimit(100)
                .currentValue(500)
                .build();
        assertFalse(measurement.isMeasurementWithinLimits());
    }

    @Test
    public void canCreateAValidMetricMeasurementWithValueLowerThanLowerLimit() {
        ImmutableCassandraHealthMetricMeasurement measurement = ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(20)
                .upperLimit(100)
                .currentValue(10)
                .build();
        assertFalse(measurement.isMeasurementWithinLimits());
    }

    @Test
    public void canCreateAValidMetricMeasurementWithValueOWithinLimits() throws Exception {
        ImmutableCassandraHealthMetricMeasurement measurement = ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(20)
                .upperLimit(100)
                .currentValue(80)
                .build();
        assertTrue(measurement.isMeasurementWithinLimits());
    }

    @Test
    public void canCreateAValidMetricMeasurementWithValueEqualToLowerLimit() throws Exception {
        ImmutableCassandraHealthMetricMeasurement measurement = ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(20)
                .upperLimit(100)
                .currentValue(20)
                .build();
        assertTrue(measurement.isMeasurementWithinLimits());
    }

    @Test
    public void canCreateAValidMetricMeasurementWithValueEqualToUpperLimit() throws Exception {
        ImmutableCassandraHealthMetricMeasurement measurement = ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(20)
                .upperLimit(100)
                .currentValue(100)
                .build();
        assertTrue(measurement.isMeasurementWithinLimits());
    }

    @Test
    public void canNotCreateAnInvalidMetricMeasurement() {
        assertThatThrownBy(() -> ImmutableCassandraHealthMetricMeasurement.builder()
                .lowerLimit(200)
                .upperLimit(100)
                .currentValue(80)
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Lower limit should be less than or equal to the upper limit."
                        + " Found LowerLimit: 200.0 and UpperLimit: 100.0.");
    }
}
