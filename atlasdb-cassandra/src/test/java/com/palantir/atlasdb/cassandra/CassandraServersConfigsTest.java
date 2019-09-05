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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;

import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;

public class CassandraServersConfigsTest {

    private static final InetSocketAddress THRIFT_SERVER_1 = new InetSocketAddress("foo", 44);
    private static final InetSocketAddress THRIFT_SERVER_2 = new InetSocketAddress("bar", 44);

    private static final CassandraServersConfigs.CqlCapableConfig CQL_CAPABLE_CONFIG =
            cqlCapable(44, 45, "bar", "foo");

    public static CassandraServersConfigs.DefaultConfig defaultConfig(InetSocketAddress... thriftServers) {
        return ImmutableDefaultConfig.builder().addThrift(thriftServers).build();
    }

    public static CassandraServersConfigs.CqlCapableConfig cqlCapable(int thriftPort, int cqlPort, String... hosts) {
        return ImmutableCqlCapableConfig.builder().addHosts(hosts).cqlPort(cqlPort).thriftPort(thriftPort).build();
    }

    @Test
    public void canDeserializeMultiEntryDefault() throws IOException {
        assertThat(deserializeClassFromFile("testServersConfigDefaultMulti.yml"))
                .isEqualTo(defaultConfig(THRIFT_SERVER_1, THRIFT_SERVER_2));
    }

    @Test
    public void canDeserializeMultiEntryCqlCapable() throws IOException {
        assertThat(deserializeClassFromFile("testServersConfigCqlCapableMulti.yml"))
                .isEqualTo(CQL_CAPABLE_CONFIG);
    }

    private static CassandraServersConfig deserializeClassFromFile(String configPath) throws IOException {
        URL configUrl = CassandraServersConfig.class.getClassLoader()
                .getResource(configPath);
        return AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfig.class);
    }
}
