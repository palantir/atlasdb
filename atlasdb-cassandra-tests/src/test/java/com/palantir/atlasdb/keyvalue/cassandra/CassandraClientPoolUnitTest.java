/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public class CassandraClientPoolUnitTest {
    public static final int POOL_REFRESH_INTERVAL_SECONDS = 10;
    public static final int DEFAULT_PORT = 5000;
    public static final int OTHER_PORT = 6000;
    public static final String HOST_1 = "1.0.0.0";
    public static final String HOST_2 = "2.0.0.0";
    public static final String HOST_3 = "3.0.0.0";

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOST_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(host.getHostName());

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOST_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(host.getHostName());

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOST_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOST_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host1, host2));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOST_3);

        assertThat(resolvedHost, equalTo(new InetSocketAddress(HOST_3, DEFAULT_PORT)));
    }


    @Test(expected = UnknownHostException.class)
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOST_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOST_2, OTHER_PORT);

        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host1, host2));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOST_3);

        assertThat(resolvedHost, equalTo(host1));
    }

    private CassandraClientPool clientPoolWithServers(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, ImmutableSet.of());
    }

    private CassandraClientPool clientPoolWithServersInCurrentPool(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(ImmutableSet.of(), servers);
    }

    private CassandraClientPool clientPoolWith(
            ImmutableSet<InetSocketAddress> servers,
            ImmutableSet<InetSocketAddress> serversInPool) {
        CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
        when(config.poolRefreshIntervalSeconds()).thenReturn(POOL_REFRESH_INTERVAL_SECONDS);
        when(config.servers()).thenReturn(servers);

        CassandraClientPool cassandraClientPool = new CassandraClientPool(config);
        cassandraClientPool.currentPools = serversInPool.stream()
                .collect(Collectors.toMap(Function.identity(), address -> mock(CassandraClientPoolingContainer.class)));
        return cassandraClientPool;
    }
}
