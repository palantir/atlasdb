/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.nexus.db.pool.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig.ServiceNameConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class OracleConnectionConfigTest {

    private static final String LOGIN = "login";
    private static final String HOST = "host";
    private static final int PORT = 42;
    private static final MaskedValue PASSWORD = ImmutableMaskedValue.of("password");
    private static final String SID = "sid";
    private static final ServiceNameConfiguration SERVICE_NAME_CONFIGURATION = new ServiceNameConfiguration.Builder()
            .serviceName("serviceName")
            .namespaceOverride("namespaceOverride")
            .build();

    @Test
    public void throwsIfNeitherSidNorServiceNameConfigurationIsSpecified() {
        assertThatThrownBy(getBaseBuilder()::build)
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Both the sid and serviceNameConfiguration are absent.");
    }

    @Test
    public void throwsIfBothSidAndServiceNameConfigurationAreSpecified() {
        OracleConnectionConfig.Builder builder =
                getBaseBuilder().sid(SID).serviceNameConfiguration(SERVICE_NAME_CONFIGURATION);
        assertThatThrownBy(builder::build)
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Exactly one of sid and serviceNameConfiguration should be provided.");
    }

    @Test
    public void databaseUrlGeneratedCorrectlyFromSid() {
        OracleConnectionConfig connectionConfig = getBaseBuilder().sid(SID).build();
        assertThat(connectionConfig.getUrl()).contains("SID=" + SID);
    }

    @Test
    public void databaseUrlGeneratedCorrectlyFromServiceName() {
        OracleConnectionConfig connectionConfig = getBaseBuilder()
                .serviceNameConfiguration(SERVICE_NAME_CONFIGURATION)
                .build();
        assertThat(connectionConfig.getUrl()).contains("SERVICE_NAME=" + SERVICE_NAME_CONFIGURATION.serviceName());
    }

    @Test
    public void namespaceIsSidIfPresent() {
        OracleConnectionConfig connectionConfig = getBaseBuilder().sid(SID).build();
        assertThat(connectionConfig.namespace()).contains(SID);
    }

    @Test
    public void namespaceIsNamespaceOverrideIfServiceNameConfigurationSpecified() {
        OracleConnectionConfig connectionConfig = getBaseBuilder()
                .serviceNameConfiguration(SERVICE_NAME_CONFIGURATION)
                .build();
        assertThat(connectionConfig.namespace()).contains(SERVICE_NAME_CONFIGURATION.namespaceOverride());
    }

    @Test
    public void settingKeyAndTrustStoresAllowedWithoutTcps() {
        OracleConnectionConfig connectionConfig = getBaseBuilder()
                .sid(SID)
                .truststorePath("truststore.jks")
                .truststorePassword("password")
                .keystorePath("keystore.jks")
                .keystorePassword("password")
                .build();
        assertThat(connectionConfig.getProtocol()).isEqualTo(ConnectionProtocol.TCP);
        assertThat(connectionConfig.getTruststorePath()).contains("truststore.jks");
        assertThat(connectionConfig.getKeystorePath()).contains("keystore.jks");
        assertThat(connectionConfig.getTruststorePassword()).contains("password");
        assertThat(connectionConfig.getKeystorePassword()).contains("password");

        // note that even though keystore/truststore are set, none of the javax.net.ssl. properties will be set
        // since the protocol is TCP
        Properties hikariProperties = connectionConfig.getHikariProperties();
        assertThat(hikariProperties.stringPropertyNames()).noneMatch(prop -> prop.startsWith("javax.net.ssl."));
    }

    @Test
    public void tcpsSetsTrustStore() throws IOException {
        File truststoreFile = new File("truststore.jks");
        boolean truststoreFileCreated = truststoreFile.createNewFile();
        try {
            OracleConnectionConfig connectionConfig = getBaseBuilder()
                    .sid(SID)
                    .protocol(ConnectionProtocol.TCPS)
                    .truststorePath("truststore.jks")
                    .truststorePassword("password")
                    .keystorePath("keystore.jks")
                    .keystorePassword("password")
                    .build();

            assertThat(connectionConfig.getProtocol()).isEqualTo(ConnectionProtocol.TCPS);
            assertThat(connectionConfig.getTruststorePath()).contains("truststore.jks");
            assertThat(connectionConfig.getKeystorePath()).contains("keystore.jks");
            assertThat(connectionConfig.getTruststorePassword()).contains("password");
            assertThat(connectionConfig.getKeystorePassword()).contains("password");

            Properties hikariProperties = connectionConfig.getHikariProperties();
            assertThat(hikariProperties)
                    .containsEntry("javax.net.ssl.trustStore", truststoreFile.getAbsolutePath())
                    .containsEntry("javax.net.ssl.trustStorePassword", "password")
                    .doesNotContainKeys("javax.net.ssl.keyStore", "javax.net.ssl.keyStorePassword");
        } finally {
            // clean up the truststore file if we created it
            if (truststoreFileCreated) {
                truststoreFile.delete();
            }
        }
    }

    private static OracleConnectionConfig.Builder getBaseBuilder() {
        return new OracleConnectionConfig.Builder()
                .dbPassword(PASSWORD)
                .dbLogin(LOGIN)
                .host(HOST)
                .port(PORT);
    }

    @Test
    public void protocolCaseInsensitiveTest() throws IOException {
        // protocol (tcp or tcps) should be case insensitive in config
        ObjectMapper mapper = new ObjectMapper();

        Map<String, ConnectionProtocol> serializedProtocols = ImmutableMap.<String, ConnectionProtocol>builder()
                .put("\"TCP\"", ConnectionProtocol.TCP)
                .put("\"tcp\"", ConnectionProtocol.TCP)
                .put("\"TCPS\"", ConnectionProtocol.TCPS)
                .put("\"tcps\"", ConnectionProtocol.TCPS)
                .build();

        for (Map.Entry<String, ConnectionProtocol> entry : serializedProtocols.entrySet()) {
            ConnectionProtocol protocol = mapper.readValue(entry.getKey(), ConnectionProtocol.class);
            assertThat(protocol).isEqualTo(entry.getValue());
        }
    }

    @Test
    public void invalidProtocolTest() {
        ObjectMapper mapper = new ObjectMapper();

        assertThatThrownBy(() -> mapper.readValue("\"invalid\"", ConnectionProtocol.class))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid does not correspond to a known ConnectionProtocol");
    }
}
