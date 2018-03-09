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

import java.nio.file.Paths;

import org.junit.Test;

import com.palantir.atlasdb.qos.config.ImmutableQosCassandraMetricsInstallConfig;
import com.palantir.atlasdb.qos.config.ImmutableQosServiceInstallConfig;
import com.palantir.atlasdb.qos.ratelimit.ThrottlingStrategyEnum;
import com.palantir.remoting.api.config.service.ServiceConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

public class QosServiceInstallConfigTest {
    @Test
    public void canBuildDefaultConfigWithNoCassandraService() {
        ImmutableQosServiceInstallConfig installConfig = ImmutableQosServiceInstallConfig.builder().build();
        assertThat(installConfig.qosCassandraMetricsConfig()).isNotPresent();
    }

    @Test
    public void canBuildWithCassandraServiceConfig() {
        ImmutableQosServiceInstallConfig.builder()
                .qosCassandraMetricsConfig(getCassandraMetricsConfig())
                .build();
    }

    private ImmutableQosCassandraMetricsInstallConfig getCassandraMetricsConfig() {
        return ImmutableQosCassandraMetricsInstallConfig.builder()
                .cassandraServiceConfig(ServiceConfiguration.builder()
                        .addUris("uri1")
                        .security(SslConfiguration.of(Paths.get("trustStore.jks")))
                        .build())
                .throttlingStrategy(ThrottlingStrategyEnum.SIMPLE)
                .build();
    }
}
