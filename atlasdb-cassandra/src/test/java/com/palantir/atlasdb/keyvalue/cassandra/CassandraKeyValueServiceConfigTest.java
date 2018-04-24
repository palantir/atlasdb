/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Paths;

import org.junit.Test;

import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

public class CassandraKeyValueServiceConfigTest {
    private static final InetSocketAddress SERVER_ADDRESS = InetSocketAddress.createUnresolved("localhost", 9160);
    private static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(Paths.get("./trustStore.jks"));

    private static final ImmutableCassandraKeyValueServiceConfig CASSANDRA_CONFIG =
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .addServers(SERVER_ADDRESS)
                    .replicationFactor(1)
                    .keyspace("atlasdb")
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
