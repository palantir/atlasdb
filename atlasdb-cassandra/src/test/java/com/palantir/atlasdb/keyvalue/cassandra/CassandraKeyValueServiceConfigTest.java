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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.cassandra.CassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import org.junit.Test;

public class CassandraKeyValueServiceConfigTest {
    private static final InetSocketAddress SERVER_ADDRESS = InetSocketAddress.createUnresolved("localhost", 9160);
    private static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(Paths.get("./trustStore.jks"));
    private static final CassandraCredentialsConfig CREDENTIALS = ImmutableCassandraCredentialsConfig.builder()
            .username("username")
            .password("password")
            .build();

    private static final ImmutableCassandraKeyValueServiceConfig CASSANDRA_CONFIG =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(ImmutableDefaultConfig.builder()
                            .addThriftHosts(SERVER_ADDRESS)
                            .build())
                    .replicationFactor(1)
                    .keyspace("atlasdb")
                    .credentials(CREDENTIALS)
                    .build();

    @Test
    public void usingSslIfSslParamPresentAndTrue() {
        assertThat(CASSANDRA_CONFIG.withSsl(true).usingSsl()).isTrue();
    }

    @Test
    public void notUsingSslIfSslParamPresentAndFalse() {
        assertThat(CASSANDRA_CONFIG.withSsl(false).usingSsl()).isFalse();
    }

    @Test
    public void notUsingSslIfSslParamFalseAndSslConfigurationPresent() {
        assertThat(CASSANDRA_CONFIG
                        .withSsl(false)
                        .withSslConfiguration(SSL_CONFIGURATION)
                        .usingSsl())
                .isFalse();
    }

    @Test
    public void usingSslIfSslParamNotPresentAndSslConfigurationPresent() {
        assertThat(CASSANDRA_CONFIG.withSslConfiguration(SSL_CONFIGURATION).usingSsl())
                .isTrue();
    }

    @Test
    public void notUsingSslIfSslParamNotPresentAndSslConfigurationNotPresent() {
        assertThat(CASSANDRA_CONFIG.usingSsl()).isFalse();
    }
}
