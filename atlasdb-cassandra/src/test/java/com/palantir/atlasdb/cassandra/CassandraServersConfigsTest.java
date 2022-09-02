/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import static com.palantir.atlasdb.cassandra.backup.BackupTestUtils.TEST_THRIFT_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.cassandra.backup.BackupTestUtils;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import org.junit.Test;

public class CassandraServersConfigsTest {
    private static final InetSocketAddress THRIFT_SERVER_1 =
            InetSocketAddress.createUnresolved("foo", TEST_THRIFT_PORT);
    private static final InetSocketAddress THRIFT_SERVER_2 =
            InetSocketAddress.createUnresolved("bar", TEST_THRIFT_PORT);

    public static final CqlCapableConfig CQL_CAPABLE_CONFIG = BackupTestUtils.cqlCapableConfig("bar", "foo");

    private static CassandraServersConfigs.DefaultConfig defaultConfig(InetSocketAddress... thriftServers) {
        return ImmutableDefaultConfig.builder().addThriftHosts(thriftServers).build();
    }

    @Test
    public void testCanDeserializeMultiEntryDefault() throws IOException {
        assertThat(deserializeClassFromFile("testServersConfigDefaultMulti.yml"))
                .isEqualTo(defaultConfig(THRIFT_SERVER_1, THRIFT_SERVER_2));
    }

    @Test
    public void testCanDeserializeMultiEntryCqlCapable() throws IOException {
        assertThat(deserializeClassFromFile("testServersConfigCqlCapableMulti.yml"))
                .isEqualTo(CQL_CAPABLE_CONFIG);
    }

    @Test
    public void testHostAreSame() throws IOException {
        CqlCapableConfig cqlCapableConfig =
                (CqlCapableConfig) deserializeClassFromFile("testServersConfigCqlCapableMulti.yml");
        assertThat(cqlCapableConfig.validateHosts()).isTrue();
    }

    @Test
    public void testHostNotTheSame() throws IOException {
        CqlCapableConfig cqlCapableConfig =
                (CqlCapableConfig) deserializeClassFromFile("testServersConfigCqlCapableDifferent.yml");
        assertThat(cqlCapableConfig.validateHosts()).isFalse();
    }

    private static CassandraServersConfig deserializeClassFromFile(String configPath) throws IOException {
        URL configUrl = CassandraServersConfig.class.getClassLoader().getResource(configPath);
        return AtlasDbConfigs.OBJECT_MAPPER.readValue(new File(configUrl.getPath()), CassandraServersConfig.class);
    }
}
