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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.junit.Test;

public class PostgresConfigLoadingTest {
    @Test
    public void testLoadingConfig() throws IOException {
        getPostgresTestConfig();
    }

    @Test
    public void testHikariSocketTimeout() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        verifyHikariProperty(connectionConfig, "socketTimeout", connectionConfig.getSocketTimeoutSeconds());
    }

    @Test
    public void testHikariConnectTimeout() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        verifyHikariProperty(connectionConfig, "connectTimeout", connectionConfig.getConnectionTimeoutSeconds());
    }

    @Test
    public void testHikariConnectionTimeout() throws IOException {
        // Hikari uses "connectionTimeout" for how long the client will wait for a connection from the pool.
        // In our parlance, this is checkoutTimeout
        // Our connectionTimeout is instead translated to "connectTimeout", which is how long a connection
        // can be open for.
        ConnectionConfig connectionConfig = getConnectionConfig();
        assertThat(connectionConfig.getHikariConfig().getConnectionTimeout())
                .isEqualTo((long) connectionConfig.getCheckoutTimeout());
    }

    @Test
    public void testHikariLoginTimeout() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        verifyHikariProperty(connectionConfig, "loginTimeout", connectionConfig.getConnectionTimeoutSeconds());
    }

    @Test
    public void testAdditionalConnectionParameters() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        Properties props = connectionConfig.getHikariProperties();

        assertThat(props.getProperty("foo")).isEqualTo("100");
        assertThat(props.getProperty("bar")).isEqualTo("baz");
    }

    @Test
    public void testPasswordIsMasked() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        assertThat(connectionConfig.getDbPassword().unmasked()).isEqualTo("testpassword");
        assertThat(connectionConfig.getHikariProperties().getProperty("password"))
                .isEqualTo("testpassword");
        assertThat(connectionConfig.toString()).doesNotContain("testpassword");
        assertThat(connectionConfig.toString()).contains("REDACTED");
    }

    private ConnectionConfig getConnectionConfig() throws IOException {
        AtlasDbConfig config = getPostgresTestConfig();
        KeyValueServiceConfig keyValueServiceConfig = config.keyValueService();
        DbKeyValueServiceConfig dbkvsConfig = (DbKeyValueServiceConfig) keyValueServiceConfig;
        return dbkvsConfig.connection();
    }

    private AtlasDbConfig getPostgresTestConfig() throws IOException {
        // Palantir internal runs this test from the jar rather than from source. This means that the resource
        // cannot be loaded as a file. Instead it must be loaded as a stream.
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("postgresTestConfig.yml")) {
            return AtlasDbConfigs.load(stream, AtlasDbConfig.class);
        }
    }

    private void verifyHikariProperty(ConnectionConfig connectionConfig, String property, int expectedValueSeconds) {
        Properties hikariProps = connectionConfig.getHikariConfig().getDataSourceProperties();

        assertThat(Integer.valueOf(hikariProps.getProperty(property)))
                .describedAs("Hikari property %s should be populated from connectionConfig", property)
                .isEqualTo(expectedValueSeconds);
    }
}
