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
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbConfigs;

public class CassandraServersConfigsTest {

    public static final InetSocketAddress THRIFT_SERVER_1 = new InetSocketAddress("foo", 44);
    public static final InetSocketAddress THRIFT_SERVER_2 = new InetSocketAddress("bar", 44);
    public static final Iterable<InetSocketAddress> THRIFT_SERVERS = ImmutableSet.of(THRIFT_SERVER_1, THRIFT_SERVER_2);

    public static final CassandraServersConfigs.CqlCapableConfig.CqlCapableServer CQL_CAPABLE_SERVER_1 =
            CassandraServersConfigs.cqlCapableServer("foo", 44, 45);
    public static final CassandraServersConfigs.CqlCapableConfig.CqlCapableServer CQL_CAPABLE_SERVER_2 =
            CassandraServersConfigs.cqlCapableServer("bar", 44, 45);
    public static final Iterable<CassandraServersConfigs.CqlCapableConfig.CqlCapableServer> CQL_CQPABLE_SERVERS =
            ImmutableSet.of(CQL_CAPABLE_SERVER_1, CQL_CAPABLE_SERVER_2);

    @Test
    public void canDeserializeOneEntryDefault() throws IOException, URISyntaxException {
        CassandraServersConfigs.DefaultConfig expected = CassandraServersConfigs.defaultConfig(THRIFT_SERVER_1);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigDefaultSingle.yml");
        CassandraServersConfigs.DefaultConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.DefaultConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeMultiEntryDefault() throws IOException, URISyntaxException {
        CassandraServersConfigs.DefaultConfig expected = CassandraServersConfigs.defaultConfig(THRIFT_SERVERS);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigDefaultMulti.yml");
        CassandraServersConfigs.DefaultConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.DefaultConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeOneEntryThrift() throws IOException, URISyntaxException {
        CassandraServersConfigs.ThriftOnlyConfig expected = CassandraServersConfigs.thriftOnlyConfig(THRIFT_SERVER_1);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigThriftSingle.yml");
        CassandraServersConfigs.ThriftOnlyConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.ThriftOnlyConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeMultiEntryThrift() throws IOException, URISyntaxException {
        CassandraServersConfigs.ThriftOnlyConfig expected = CassandraServersConfigs.thriftOnlyConfig(THRIFT_SERVERS);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigThriftMulti.yml");
        CassandraServersConfigs.ThriftOnlyConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.ThriftOnlyConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeSingleEntryCqlCapable() throws IOException, URISyntaxException {
        CassandraServersConfigs.CqlCapableConfig expected =
                CassandraServersConfigs.cqlCapableConfig(CQL_CAPABLE_SERVER_1);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigCqlCapableSingle.yml");
        CassandraServersConfigs.CqlCapableConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.CqlCapableConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeMultiEntryCqlCapable() throws IOException, URISyntaxException {
        CassandraServersConfigs.CqlCapableConfig expected =
                CassandraServersConfigs.cqlCapableConfig(CQL_CQPABLE_SERVERS);
        URL configUrl = CassandraKeyValueServiceConfigsTest.class.getClassLoader()
                .getResource("testServersConfigCqlCapableMulti.yml");
        CassandraServersConfigs.CqlCapableConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), CassandraServersConfigs.CqlCapableConfig.class);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }
}
