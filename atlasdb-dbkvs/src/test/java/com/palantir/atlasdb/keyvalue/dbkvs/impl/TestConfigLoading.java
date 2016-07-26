/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;

public class TestConfigLoading {
    @Test
    public void testLoadingConfig() throws IOException {
        AtlasDbConfigs.load(new File(getClass().getClassLoader().getResource("postgresTestConfig.yml").getFile()));
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
        // Hikari uses "connectionTimeout" for how long the client will wait for a connection from the pool. In our parlance, this is checkoutTimeout
        // Our connectionTimeout is instead translated to "connectTimeout", which is how long a connection can be open for.
        ConnectionConfig connectionConfig = getConnectionConfig();
        assertThat(connectionConfig.getHikariConfig().getConnectionTimeout(), is((long) connectionConfig.getCheckoutTimeout()));
    }

    @Test
    public void testHikariLoginTimeout() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        verifyHikariProperty(connectionConfig, "loginTimeout", connectionConfig.getConnectionTimeoutSeconds());
    }

    private ConnectionConfig getConnectionConfig() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(new File(getClass().getClassLoader().getResource("postgresTestConfig.yml").getFile()));
        KeyValueServiceConfig keyValueServiceConfig = config.keyValueService();
        DbKeyValueServiceConfig dbkvsConfig = (DbKeyValueServiceConfig) keyValueServiceConfig;
        return dbkvsConfig.connection();
    }

    private void verifyHikariProperty(ConnectionConfig connectionConfig, String property, int expectedValueSeconds) {
        Properties hikariProps = connectionConfig.getHikariConfig().getDataSourceProperties();

        assertThat(
                String.format("Hikari property %s should be populated from connectionConfig", property),
                Integer.valueOf(hikariProps.getProperty(property)),
                is(expectedValueSeconds));
    }
}
