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
    public static final Iterable<CassandraServersConfigs.CqlCapableConfig.CqlCapableServer> CQL_CAPABLE_SERVERS =
            ImmutableSet.of(CQL_CAPABLE_SERVER_1, CQL_CAPABLE_SERVER_2);

    public static void helperTestMethod(CassandraServersConfigs.CassandraServersConfig expected, String configPath,
            Class<? extends CassandraServersConfigs.CassandraServersConfig> deserializationClass) throws IOException {
        URL configUrl = deserializationClass.getClassLoader()
                .getResource(configPath);
        CassandraServersConfigs.CassandraServersConfig deserializedServersConfig = AtlasDbConfigs.OBJECT_MAPPER
                .readValue(new File(configUrl.getPath()), deserializationClass);

        assertThat(deserializedServersConfig).isEqualTo(expected);
    }

    @Test
    public void canDeserializeOneEntryDefault() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.defaultConfig(THRIFT_SERVER_1),
                "testServersConfigDefaultSingle.yml",
                CassandraServersConfigs.DefaultConfig.class);
    }

    @Test
    public void canDeserializeMultiEntryDefault() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.defaultConfig(THRIFT_SERVERS),
                "testServersConfigDefaultMulti.yml",
                CassandraServersConfigs.DefaultConfig.class);
    }

    @Test
    public void canDeserializeOneEntryThrift() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.thriftOnly(THRIFT_SERVER_1),
                "testServersConfigThriftSingle.yml",
                CassandraServersConfigs.ThriftOnlyConfig.class);
    }

    @Test
    public void canDeserializeMultiEntryThrift() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.thriftOnly(THRIFT_SERVERS),
                "testServersConfigThriftMulti.yml",
                CassandraServersConfigs.ThriftOnlyConfig.class);
    }

    @Test
    public void canDeserializeSingleEntryCqlCapable() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.cqlCapable(CQL_CAPABLE_SERVER_1),
                "testServersConfigCqlCapableSingle.yml",
                CassandraServersConfigs.CqlCapableConfig.class);
    }

    @Test
    public void canDeserializeMultiEntryCqlCapable() throws IOException, URISyntaxException {
        helperTestMethod(
                CassandraServersConfigs.cqlCapable(CQL_CAPABLE_SERVERS),
                "testServersConfigCqlCapableMulti.yml",
                CassandraServersConfigs.CqlCapableConfig.class);
    }
}
