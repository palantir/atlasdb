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

import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CassandraServersConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.CqlCapableConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class CassandraServersConfigsTest {

    private static final int TEST_THRIFT_PORT = 44;
    private static final int TEST_CQL_PORT = 45;

    private static final InetSocketAddress THRIFT_SERVER_1 = new InetSocketAddress("foo", TEST_THRIFT_PORT);
    private static final InetSocketAddress THRIFT_SERVER_2 = new InetSocketAddress("bar", TEST_THRIFT_PORT);

    private static final CqlCapableConfig CQL_CAPABLE_CONFIG =
            cqlCapable("bar", "foo");

    private static CassandraServersConfigs.DefaultConfig defaultConfig(InetSocketAddress... thriftServers) {
        return ImmutableDefaultConfig.builder().addThriftHosts(thriftServers).build();
    }

    private static CqlCapableConfig cqlCapable(String... hosts) {
        Iterable<InetSocketAddress> thriftHosts = constructHosts(TEST_THRIFT_PORT, hosts);
        Iterable<InetSocketAddress> cqlHosts = constructHosts(TEST_CQL_PORT, hosts);
        return ImmutableCqlCapableConfig.builder()
                .cqlHosts(cqlHosts)
                .thriftHosts(thriftHosts)
                .build();
    }

    private static List<InetSocketAddress> constructHosts(int port, String[] hosts) {
        return Stream.of(hosts).map(host -> new InetSocketAddress(host, port)).collect(Collectors.toList());
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
        URL configUrl = CassandraServersConfig.class.getClassLoader()
                .getResource(configPath);
        return AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfig.class);
    }
}
