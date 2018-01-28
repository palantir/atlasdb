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

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class HealthMetricsTest {
    private static final String TEST_NAME = "metricName";
    private static final String TEST_TYPE = "metricType";
    private static final String TEST_ATTRIBUTE = "attribute";

    @Test
    public void canCreateMetricMeasurementWithLowerLimitLessThanUpperLimit() {
        ImmutableHealthMetric.builder()
                .lowerLimit(10)
                .upperLimit(20)
                .name(TEST_NAME)
                .type(TEST_TYPE)
                .attribute(TEST_ATTRIBUTE)
                .additionalParams(ImmutableMap.of())
                .build();
    }

    @Test
    public void canNotCreateMetricMeasurementWithLowerLimitMoreThanUpperLimit() {
        assertThatThrownBy(() -> ImmutableHealthMetric.builder()
                .lowerLimit(20)
                .upperLimit(10)
                .name(TEST_NAME)
                .type(TEST_TYPE)
                .attribute(TEST_ATTRIBUTE)
                .additionalParams(ImmutableMap.of())
                .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Lower limit should be less than or equal to the upper limit."
                        + " Found LowerLimit: 20.0 and UpperLimit: 10.0.");
    }
}
