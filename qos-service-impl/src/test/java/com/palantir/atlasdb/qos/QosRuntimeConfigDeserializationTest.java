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
import com.palantir.atlasdb.qos.config.ImmutableQosClientLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.config.QosPriority;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;

public class QosRuntimeConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule());

    @Test
    public void canDeserializeQosServerConfiguration() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource(
                "/qos-server-without-cassandra-metrics.yml").getPath());
        QosServiceRuntimeConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceRuntimeConfig.class);

        assertThat(configuration).isEqualTo(ImmutableQosServiceRuntimeConfig.builder()
                .clientLimits(ImmutableMap.of("test", getQosClientLimitsConfig(10, 20, QosPriority.HIGH),
                        "test2", getQosClientLimitsConfig(30, 40, QosPriority.MEDIUM),
                        "test3", getQosClientLimitsConfig(50, 50, QosPriority.LOW)))
                .build());
    }

    private ImmutableQosClientLimitsConfig getQosClientLimitsConfig(long readLimit,
            long writeLimit,
            QosPriority priority) {
        return ImmutableQosClientLimitsConfig.builder()
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(readLimit)
                        .writeBytesPerSecond(writeLimit)
                        .build())
                .clientPriority(priority)
                .build();
    }
}
