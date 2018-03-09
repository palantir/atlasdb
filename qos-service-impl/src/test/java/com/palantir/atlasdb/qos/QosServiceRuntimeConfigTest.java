/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.qos;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableHealthMetric;
import com.palantir.atlasdb.qos.config.ImmutableQosCassandraMetricsRuntimeConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;

@SuppressWarnings("CheckReturnValue") // We use Immutables builder.build() to check failing fast (or not)...
public class QosServiceRuntimeConfigTest {
    @Test
    public void canBuildFromEmptyClientLimitsWithoutCasandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder().clientLimits(ImmutableMap.of()).build();
    }

    @Test
    public void canBuildFromEmptyClientLimitsWithCasandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder().clientLimits(ImmutableMap.of())
                .qosCassandraMetricsConfig(getCassandraMetricsConfig())
                .build();
    }

    @Test
    public void canBuildFromSingleClientLimitWithoutCasandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test_client", getQosClientLimitsConfig()))
                .qosCassandraMetricsConfig(getCassandraMetricsConfig());
    }

    @Test
    public void canBuildFromSingleClientLimitWithCasandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test_client", getQosClientLimitsConfig()))
                .build();
    }

    @Test
    public void canBuildFromMultipleClientLimitsWithoutCassandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test_client", getQosClientLimitsConfig(),
                        "test_client2", getQosClientLimitsConfig()))
                .build();
    }

    @Test
    public void canBuildFromMultipleClientLimitsWithCassandraMetricsConfig() {
        ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test_client", getQosClientLimitsConfig(),
                        "test_client2", getQosClientLimitsConfig()))
                .qosCassandraMetricsConfig(getCassandraMetricsConfig())
                .build();
    }

    private ImmutableQosCassandraMetricsRuntimeConfig getCassandraMetricsConfig() {
        return ImmutableQosCassandraMetricsRuntimeConfig.builder()
                .cassandraHealthMetrics(ImmutableList.of(ImmutableHealthMetric.builder()
                        .type("CommitLog")
                        .name("PendingTasks")
                        .attribute("Value")
                        .lowerLimit(0)
                        .upperLimit(50)
                        .build()))
                .build();
    }

    private ImmutableQosClientLimitsConfig getQosClientLimitsConfig() {
        return ImmutableQosClientLimitsConfig.builder()
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(10L)
                        .writeBytesPerSecond(10L)
                        .build())
                .build();
    }
}
