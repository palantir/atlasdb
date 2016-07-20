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

import org.junit.Test;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.zaxxer.hikari.HikariConfig;

public class TestConfigLoading {
    @Test
    public void testLoadingConfig() throws IOException {
        AtlasDbConfigs.load(new File(getClass().getClassLoader().getResource("postgresTestConfig.yml").getFile()));
    }

    @Test
    public void testHikariDefaults() throws IOException {
        AtlasDbConfig config = AtlasDbConfigs.load(new File(getClass().getClassLoader().getResource("postgresTestConfig.yml").getFile()));
        KeyValueServiceConfig keyValueServiceConfig = config.keyValueService();
        DbKeyValueServiceConfig dbkvsConfig = (DbKeyValueServiceConfig) keyValueServiceConfig;
        ConnectionConfig connectionConfig = dbkvsConfig.connection();
        HikariConfig hikariConfig = connectionConfig.getHikariConfig();

        // TODO
//        assertThat(
//                "Hikari socket timeout should be populated from connectionConfig",
//                hikariConfig.getConnectionTimeout(),
//                is(connectionConfig.getConnectionTimeoutSeconds() * 1000));

        assertThat(
                "Hikari connection timeout should be populated from connectionConfig",
                hikariConfig.getConnectionTimeout(),
                is(connectionConfig.getConnectionTimeoutSeconds() * 1000L));

//        assertThat(
//                "Hikari login timeout should be populated from connectionConfig",
//                hikariConfig.getConnectionTimeout(),
//                is(connectionConfig.getConnectionTimeoutSeconds() * 1000));
    }
}
