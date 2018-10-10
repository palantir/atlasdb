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
import java.nio.file.Paths;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.qos.config.ImmutableQosClientConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosLimitsConfig;
import com.palantir.atlasdb.qos.config.QosClientConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.remoting.api.config.service.HumanReadableDuration;
import com.palantir.remoting.api.config.service.ServiceConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.remoting3.ext.jackson.ShimJdk7Module;

public class QosClientConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule())
            .registerModule(new ShimJdk7Module())
            .registerModule(new Jdk8Module());

    @Test
    public void canDeserializeFromYaml() throws IOException {
        QosClientConfig expected = ImmutableQosClientConfig.builder()
                .qosService(
                        ServiceConfiguration.builder()
                                .addUris("http://localhost:8080")
                                .security(SslConfiguration.of(Paths.get("trustStore.jks")))
                                .build())
                .maxBackoffSleepTime(HumanReadableDuration.seconds(20))
                .limits(ImmutableQosLimitsConfig.builder()
                        .readBytesPerSecond(123)
                        .writeBytesPerSecond(456)
                        .build())
                .build();

        File configFile = new File(QosServiceRuntimeConfig.class.getResource("/qos-client.yml").getPath());
        QosClientConfig config = OBJECT_MAPPER.readValue(configFile, QosClientConfig.class);

        assertThat(config).isEqualTo(expected);
    }

}
