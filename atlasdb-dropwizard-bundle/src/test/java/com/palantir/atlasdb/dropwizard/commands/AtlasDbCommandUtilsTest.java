/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.dropwizard.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ImmutableTimeLockClientConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.remoting2.config.ssl.SslConfiguration;

public class AtlasDbCommandUtilsTest {
    private static final String LOCAL_SERVER_NAME = "Local Server";
    private static final AtlasDbConfig MINIMAL_LEADER_CONFIG = ImmutableAtlasDbConfig.builder()
            .leader(ImmutableLeaderConfig.builder()
                    .quorumSize(1)
                    .addLeaders(LOCAL_SERVER_NAME)
                    .localServer(LOCAL_SERVER_NAME)
                    .build())
            .keyValueService(mock(KeyValueServiceConfig.class))
            .build();
    private static final AtlasDbConfig MINIMAL_EMBEDDED_CONFIG = ImmutableAtlasDbConfig.builder()
            .keyValueService(mock(KeyValueServiceConfig.class))
            .build();
    private static final ServerListConfig LOCAL_SERVER_LIST_CONFIG = ImmutableServerListConfig.builder()
            .addServers(LOCAL_SERVER_NAME)
            .build();
    private static final TimeLockClientConfig TIME_LOCK_CLIENT_CONFIG = ImmutableTimeLockClientConfig.builder()
            .client(LOCAL_SERVER_NAME)
            .serversList(LOCAL_SERVER_LIST_CONFIG)
            .build();
    private static final AtlasDbConfig TIME_LOCK_CONFIG = ImmutableAtlasDbConfig.builder()
            .timelock(TIME_LOCK_CLIENT_CONFIG)
            .keyValueService(mock(KeyValueServiceConfig.class))
            .build();

    @Test
    public void leaderBlockNoLongerExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.leader().isPresent()).isFalse();
    }

    @Test
    public void timestampBlockExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.timestamp().isPresent()).isTrue();
    }

    @Test
    public void timestampBlockContainsLeadersAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.timestamp().get().servers()).containsExactly(LOCAL_SERVER_NAME);
    }

    @Test
    public void lockBlockExistsAfterConvertingConfig() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_LEADER_CONFIG);

        assertThat(clientConfig.lock().get().servers()).containsExactly(LOCAL_SERVER_NAME);
    }

    @Test
    public void clientConfigMatchesServerConfigForTimelock() {
        AtlasDbConfig clientConfig = AtlasDbCommandUtils.convertServerConfigToClientConfig(TIME_LOCK_CONFIG);

        assertThat(clientConfig).isEqualTo(TIME_LOCK_CONFIG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void conversionFailsWhenUsingEmbeddedServerConfig() {
        AtlasDbCommandUtils.convertServerConfigToClientConfig(MINIMAL_EMBEDDED_CONFIG);
    }

    @Test
    public void argumentsWithoutTwoHyphensAtTheBeginningAreFilteredOut() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("unrelated-arg", "some-value"));

        assertThat(gatheredArgs).isEmpty();
    }

    @Test
    public void argumentsWhichHaveNullValuesAreFilteredOut() {
        Map<String, Object> args = new HashMap<>();
        args.put("--null-arg", null);

        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(args);

        assertThat(gatheredArgs).isEmpty();
    }

    @Test
    public void argumentsWhichDontHaveNullValuesAreKept() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--non-null-arg", "123"));

        assertThat(gatheredArgs).containsExactly("--non-null-arg", "123");
    }

    @Test
    public void argumentsWhichAreListsAreInlined() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--list-arg", ImmutableList.of("123", "456")));

        assertThat(gatheredArgs).containsExactly("--list-arg", "123", "456");
    }

    @Test
    public void argumentsWithTheZeroArityStringHaveOnlyTheKeyKept() {
        List<String> gatheredArgs = AtlasDbCommandUtils.gatherPassedInArguments(
                ImmutableMap.of("--zero-arity-arg", AtlasDbCommandUtils.ZERO_ARITY_ARG_CONSTANT));

        assertThat(gatheredArgs).containsExactly("--zero-arity-arg");
    }

    @Test
    public void canSerializeAndDeserializeAtlasDbConfig() throws IOException {
        SslConfiguration ssl = SslConfiguration.of(
                new File("var/security/truststore.jks").toPath(),
                new File("var/security/keystore.jks").toPath(),
                "keystorePassword");
        @SuppressWarnings("deprecation")
        AtlasDbConfig bigConfig = ImmutableAtlasDbConfig.builder()
                .leader(ImmutableLeaderConfig.builder()
                        .quorumSize(1)
                        .addLeaders(LOCAL_SERVER_NAME)
                        .localServer(LOCAL_SERVER_NAME)
                        .sslConfiguration(ssl)
                        // jackson serializes files to absolute file path so we need to
                        // getAbsoluteFile() to ensure the equals works at the end
                        .learnerLogDir(new File("var/data/paxos/learner").getAbsoluteFile())
                        .acceptorLogDir(new File("var/data/paxos/acceptor").getAbsoluteFile())
                        .build())
                .keyValueService(ImmutableCassandraKeyValueServiceConfig.builder()
                        .keyspace("test")
                        .replicationFactor(3)
                        .servers(ImmutableSet.of(
                                new InetSocketAddress("host1", 9160),
                                new InetSocketAddress("host2", 9160),
                                new InetSocketAddress("host3", 9160)))
                        .ssl(true)
                        .sslConfiguration(ssl)
                        .build())
                .build();
        String configAsString = AtlasDbCommandUtils.serialiseConfiguration(bigConfig);
        AtlasDbConfig deserializedConfig = AtlasDbConfigs.loadFromString(configAsString, "");

        assertThat(bigConfig).isEqualTo(deserializedConfig);
    }
}
