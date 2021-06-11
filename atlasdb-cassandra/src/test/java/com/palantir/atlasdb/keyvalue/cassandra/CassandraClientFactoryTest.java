/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManagers;
import java.net.InetSocketAddress;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.Test;

public class CassandraClientFactoryTest {
    private static final CassandraClientFactory FACTORY = new CassandraClientFactory(
            MetricsManagers.createForTests(),
            InetSocketAddress.createUnresolved("localhost", 4242),
            ImmutableCassandraKeyValueServiceConfig.builder()
                    .replicationFactor(1)
                    .credentials(ImmutableCassandraCredentialsConfig.builder()
                            .username("jeremy")
                            .password("tom")
                            .build())
                    .build());

    private CassandraClient client = mock(CassandraClient.class);
    private PooledObject<CassandraClient> pooledClient = new DefaultPooledObject<>(client);

    @Test
    public void reliesOnOpennessOfUnderlyingTransport() {
        when(client.getOutputProtocol()).thenReturn(new TCompactProtocol(new TMemoryInputTransport(), 31337, 131072));
        assertThat(FACTORY.validateObject(pooledClient)).isTrue();
    }

    @Test
    public void doesNotPropagateExceptionsThrown() {
        when(client.getOutputProtocol()).thenThrow(new RuntimeException());
        assertThat(FACTORY.validateObject(pooledClient)).isFalse();
    }
}
