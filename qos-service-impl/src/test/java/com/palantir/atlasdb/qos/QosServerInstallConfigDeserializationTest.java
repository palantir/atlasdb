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
import com.palantir.atlasdb.qos.config.ImmutableQosCassandraMetricsInstallConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceInstallConfig;
import com.palantir.atlasdb.qos.config.QosServiceInstallConfig;
import com.palantir.atlasdb.qos.config.QosServiceRuntimeConfig;
import com.palantir.atlasdb.qos.ratelimit.ThrottlingStrategyEnum;
import com.palantir.remoting.api.config.service.ServiceConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.remoting3.ext.jackson.ShimJdk7Module;

public class QosServerInstallConfigDeserializationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new GuavaModule())
            .registerModule(new Jdk8Module())
            .registerModule(new ShimJdk7Module());

    @Test
    public void canDeserializeQosServerConfiguration() throws IOException {
        File testConfigFile = new File(QosServiceRuntimeConfig.class.getResource(
                "/qos-server-install.yml").getPath());
        QosServiceInstallConfig configuration = OBJECT_MAPPER.readValue(testConfigFile, QosServiceInstallConfig.class);

        assertThat(ImmutableQosServiceInstallConfig.builder()
                .qosCassandraMetricsConfig(ImmutableQosCassandraMetricsInstallConfig.builder()
                        .cassandraServiceConfig(ServiceConfiguration.builder()
                                .addUris("https://localhost:9161/cassandra-sidecar/api/")
                                .security(SslConfiguration.of(Paths.get("trustStore.jks")))
                                .build())
                        .throttlingStrategy(ThrottlingStrategyEnum.SIMPLE)
                        .build())
                .build())
                .isEqualTo(configuration);

    }
}
