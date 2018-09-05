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

import java.nio.file.Paths;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.qos.config.QosClientLimitsConfig;
import com.palantir.conjure.java.api.config.service.ServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.client.config.ClientConfigurations;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;

public class QosServiceIntegrationTest {
    @ClassRule
    public static QosServerHolder serverHolder = new QosServerHolder("server.yml");

    private static final String SERVER_URI = "http://localhost:5080";

    private static final SslConfiguration TEST_SSL_CONFIG =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));

    private static final ServiceConfiguration SERVICE_CONFIGURATION = ServiceConfiguration.builder()
            .security(TEST_SSL_CONFIG)
            .addUris(SERVER_URI)
            .build();

    private static QosService service = JaxRsClient.create(
            QosService.class,
            UserAgents.parse("integration tests"),
            new HostMetricsRegistry(),
            ClientConfigurations.of(SERVICE_CONFIGURATION));

    @Test
    public void returnsConfiguredLimits() {
        assertThat(service.readLimit("test")).isEqualTo(10);
        assertThat(service.writeLimit("test")).isEqualTo(20);
        assertThat(service.readLimit("test2")).isEqualTo(30);
        assertThat(service.writeLimit("test2")).isEqualTo(40);
    }

    @Test
    public void returnsDefaultLimitsForNotConfiguredClients() {
        assertThat(service.readLimit("unknown")).isEqualTo(QosClientLimitsConfig.BYTES_READ_PER_SECOND_PER_CLIENT);
        assertThat(service.writeLimit("unknown")).isEqualTo(QosClientLimitsConfig.BYTES_WRITTEN_PER_SECOND_PER_CLIENT);
    }

}
