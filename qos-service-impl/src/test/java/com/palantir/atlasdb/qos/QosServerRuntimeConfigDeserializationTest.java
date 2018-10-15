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
package com.palantir.atlasdb.qos;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.qos.config.ImmutableHealthMetric;
import com.palantir.atlasdb.qos.config.ImmutableQosCassandraMetricsRuntimeConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

public class QosServerRuntimeConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module());

    @Test
    public void canDeserializeQosServerConfiguration() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource(
                "/qos-server-without-cassandra-metrics.yml").getPath());
        QosServiceRuntimeConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceRuntimeConfig.class);

        assertThat(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(getTestClientLimits())
                .build())
                .isEqualTo(configuration);
    }

    @Test
    public void canDeserializeQosServerConfigurationWithCassandraMetrics() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource(
                "/qos-server-with-cassandra-metrics.yml").getPath());
        QosServiceRuntimeConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceRuntimeConfig.class);

        assertThat(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(getTestClientLimits())
                .qosCassandraMetricsConfig(ImmutableQosCassandraMetricsRuntimeConfig.builder()
                        .cassandraHealthMetrics(ImmutableList.of(ImmutableHealthMetric.builder()
                                .type("CommitLog")
                                .name("PendingTasks")
                                .attribute("Value")
                                .lowerLimit(0)
                                .upperLimit(50)
                                .build()))
                        .build())
                .build())
                .isEqualTo(configuration);
    }

    private ImmutableMap<String, ImmutableQosClientLimitsConfig> getTestClientLimits() {
        return ImmutableMap.of("test", getQosClientLimitsConfig(10, 20),
                "test2", getQosClientLimitsConfig(30, 40),
                "test3", getQosClientLimitsConfig(50, 50));
    }

    private ImmutableQosClientLimitsConfig getQosClientLimitsConfig(long readLimit,
            long writeLimit) {
        return ImmutableQosClientLimitsConfig.builder()
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(readLimit)
                        .writeBytesPerSecond(writeLimit)
                        .build())
                .build();
    }
}
