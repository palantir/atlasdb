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
package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.atlasdb.qos.FakeQosClient;
import com.palantir.atlasdb.util.MetricsManagers;

public class CassandraServiceTest {
    private static final int DEFAULT_PORT = 5000;
    private static final int OTHER_PORT = 6000;
    private static final String HOSTNAME_1 = "1.0.0.0";
    private static final String HOSTNAME_2 = "2.0.0.0";
    private static final String HOSTNAME_3 = "3.0.0.0";
    private static final InetSocketAddress HOST_1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
    private static final InetSocketAddress HOST_2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);

    private CassandraKeyValueServiceConfig config;
    private Blacklist blacklist;

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(HOST_1));
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(HOST_1));
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));

        InetSocketAddress resolvedHost = cassandra.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost, equalTo(new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT)));
    }


    @Test(expected = UnknownHostException.class)
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, OTHER_PORT);

        CassandraService cassandra = clientPoolWithServers(ImmutableSet.of(HOST_1, host2));

        cassandra.getAddressForHost(HOSTNAME_3);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        CassandraService cassandra = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1));

        Optional<CassandraClientPoolingContainer> container
                = cassandra.getRandomGoodHostForPredicate(address -> false);
        assertThat(container.isPresent(), is(false));
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        CassandraService cassandra = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));

        int numTrials = 50;
        for (int i = 0; i < numTrials; i++) {
            Optional<CassandraClientPoolingContainer> container
                    = cassandra.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
            assertContainerHasHostOne(container);
        }
    }

    @Test
    public void shouldNotReturnHostsNotMatchingPredicateEvenWithNodeFailure() {
        CassandraService cassandra = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));
        blacklist.add(HOST_1);
        Optional<CassandraClientPoolingContainer> container
                = cassandra.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
        assertContainerHasHostOne(container);
    }

    @SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "ConstantConditions"})
    private void assertContainerHasHostOne(Optional<CassandraClientPoolingContainer> container) {
        assertThat(container.isPresent(), is(true));
        assertThat(container.get().getHost(), equalTo(HOST_1));
    }

    private CassandraService clientPoolWithServers(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, servers);
    }

    private CassandraService clientPoolWithServersInCurrentPool(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, servers);
    }

    private CassandraService clientPoolWith(
            ImmutableSet<InetSocketAddress> servers,
            ImmutableSet<InetSocketAddress> serversInPool) {
        config = ImmutableCassandraKeyValueServiceConfig.builder()
                .replicationFactor(3)
                .addServers(servers.toArray(new InetSocketAddress[0]))
                .build();

        blacklist = new Blacklist(config);

        CassandraService service = new CassandraService(
                MetricsManagers.createForTests(),
                config, blacklist, new FakeQosClient());

        service.cacheInitialCassandraHosts();
        serversInPool.forEach(service::addPool);

        return service;
    }
}
