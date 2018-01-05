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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableCassandraHealthMetric;
import com.palantir.atlasdb.qos.config.ImmutableQosCassandraMetricsRuntimeConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

public class QosRuntimeConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule());

    @Test
    public void canDeserializeQosServerRuntimeConfiguration() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource("/qos-server.yml").getPath());
        QosServiceRuntimeConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceRuntimeConfig.class);

        assertThat(configuration).isEqualTo(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(
                        ImmutableMap.of("test", configWithLimitsAndPriority(10L, 20L, QosPriority.HIGH),
                                "test2", configWithLimitsAndPriority(30L, 40L, QosPriority.MEDIUM),
                                "test3", configWithLimitsAndPriority(50L, 50L, QosPriority.LOW)))
                .qosCassandraMetricsConfig(ImmutableQosCassandraMetricsRuntimeConfig.builder()
                        .addCassandraHealthMetrics(ImmutableCassandraHealthMetric.builder()
                                .type("CommitLog")
                                .name("PendingTasks")
                                .attribute("Value")
                                .lowerLimit(0)
                                .upperLimit(50)
                                .build())
                        .build())
                .build());
    }

    private QosClientLimitsConfig configWithLimitsAndPriority(long readLimit, long writeLimit, QosPriority priority) {
        return ImmutableQosClientLimitsConfig.builder()
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(readLimit)
                        .writeBytesPerSecond(writeLimit)
                        .build())
                .clientPriority(priority)
                .build();
    }
}
