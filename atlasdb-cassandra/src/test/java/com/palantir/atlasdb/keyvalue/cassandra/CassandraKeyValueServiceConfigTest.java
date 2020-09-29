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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    private static final CassandraCredentialsConfig CREDENTIALS =
            ImmutableCassandraCredentialsConfig.builder()
                    .username("username")
                    .password("password")
                    .build();

    private static final ImmutableCassandraKeyValueServiceConfig CASSANDRA_CONFIG =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .servers(
                            ImmutableDefaultConfig
                                    .builder().addThriftHosts(SERVER_ADDRESS).build())
                    .replicationFactor(1)
                    .keyspace("atlasdb")
                    .credentials(CREDENTIALS)
                    .build();

    @Test
    public void usingSslIfSslParamPresentAndTrue() {
        assertTrue(CASSANDRA_CONFIG.withSsl(true).usingSsl());
    }

    @Test
    public void notUsingSslIfSslParamPresentAndFalse() {
        assertFalse(CASSANDRA_CONFIG.withSsl(false).usingSsl());
    }

    @Test
    public void notUsingSslIfSslParamFalseAndSslConfigurationPresent() {
        assertFalse(CASSANDRA_CONFIG.withSsl(false).withSslConfiguration(SSL_CONFIGURATION).usingSsl());
    }

    @Test
    public void usingSslIfSslParamNotPresentAndSslConfigurationPresent() {
        assertTrue(CASSANDRA_CONFIG.withSslConfiguration(SSL_CONFIGURATION).usingSsl());
    }

    @Test
    public void notUsingSslIfSslParamNotPresentAndSslConfigurationNotPresent() {
        assertFalse(CASSANDRA_CONFIG.usingSsl());
    }
}
