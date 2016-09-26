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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;

public class TestOracleConfigLoading {

    private static final String ORACLE_PASSWORD = "palpal";

    @Test
    public void testLoadingConfig() throws IOException {
        getOracleTestConfig();
    }

    @Test
    public void testHikariConnectionTimeout() throws IOException {
        // Hikari uses "connectionTimeout" for how long the client will wait for a connection from the pool.
        // In our parlance, this is checkoutTimeout
        // Our connectionTimeout is instead translated to "connectTimeout", which is how long a connection
        // can be open for.
        ConnectionConfig connectionConfig = getConnectionConfig();
        assertThat(connectionConfig.getHikariConfig().getConnectionTimeout(),
                is((long) connectionConfig.getCheckoutTimeout()));
    }

    @Test
    public void testPasswordIsMasked() throws IOException {
        ConnectionConfig connectionConfig = getConnectionConfig();
        assertThat(connectionConfig.getDbPassword().unmasked(), equalTo(ORACLE_PASSWORD));
        assertThat(connectionConfig.getHikariProperties().getProperty("password"), equalTo(ORACLE_PASSWORD));
        assertThat(connectionConfig.toString(), not(containsString(ORACLE_PASSWORD)));
        assertThat(connectionConfig.toString(), containsString("REDACTED"));
    }

    private ConnectionConfig getConnectionConfig() throws IOException {
        AtlasDbConfig config = getOracleTestConfig();
        KeyValueServiceConfig keyValueServiceConfig = config.keyValueService();
        DbKeyValueServiceConfig dbkvsConfig = (DbKeyValueServiceConfig) keyValueServiceConfig;
        return dbkvsConfig.connection();
    }

    private AtlasDbConfig getOracleTestConfig() throws IOException {
        // Palantir internal runs this test from the jar rather than from source. This means that the resource
        // cannot be loaded as a file. Instead it must be loaded as a stream.
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("oracleTestConfig.yml")) {
            return AtlasDbConfigs.load(stream);
        }
    }
}
