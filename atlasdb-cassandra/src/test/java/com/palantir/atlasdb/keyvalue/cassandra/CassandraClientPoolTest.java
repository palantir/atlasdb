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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolTest {
    public static final int POOL_REFRESH_INTERVAL_SECONDS = 3 * 60;
    public static final int TIME_BETWEEN_EVICTION_RUNS_SECONDS = 20;
    public static final int DEFAULT_PORT = 5000;
    public static final int OTHER_PORT = 6000;
    public static final String HOSTNAME_1 = "1.0.0.0";
    public static final String HOSTNAME_2 = "2.0.0.0";
    public static final String HOSTNAME_3 = "3.0.0.0";

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host1, host2));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost, equalTo(new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT)));
    }


    @Test(expected = UnknownHostException.class)
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, OTHER_PORT);

        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(host1, host2));

        cassandraClientPool.getAddressForHost(HOSTNAME_3);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> false);
        assertThat(container.isPresent(), is(false));
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        int numTrials = 50;
        for (int i = 0; i < numTrials; i++) {
            Optional<CassandraClientPoolingContainer> container
                    = cassandraClientPool.getRandomGoodHostForPredicate(address -> address.equals(host1));
            assertThat(container.isPresent(), is(true));
            assertThat(container.get().getHost(), equalTo(host1));
        }
    }

    @Test
    public void shouldNotReturnHostsNotMatchingPredicateEvenWithNodeFailure() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        cassandraClientPool.blacklistedHosts.put(host1, System.currentTimeMillis());
        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> address.equals(host1));
        assertThat(container.isPresent(), is(true));
        assertThat(container.get().getHost(), equalTo(host1));
    }

    @Test
    public void shouldNotAttemptMoreThanOneConnectionOnSuccess() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));
        cassandraClientPool.runWithRetryOnHost(host, input -> null);
        verifyNumberOfAttemptsOnHost(host, cassandraClientPool, 1);
    }

    @Test
    public void shouldRetryOnSameNodeToFailureAndThenRedirect() {
        int numHosts = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;
        List<InetSocketAddress> hostList = Lists.newArrayList();
        for (int i = 0; i < numHosts; i++) {
            hostList.add(new InetSocketAddress(i));
        }

        CassandraClientPool cassandraClientPool = throwingClientPoolWithServersInCurrentPool(
                ImmutableSet.copyOf(hostList), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(hostList.get(0), input -> null);
            fail();
        } catch (Exception e) {
            // expected, keep going
        }

        verifyNumberOfAttemptsOnHost(hostList.get(0), cassandraClientPool, CassandraClientPool.MAX_TRIES_SAME_HOST);
        for (int i = 1; i < numHosts; i++) {
            verifyNumberOfAttemptsOnHost(hostList.get(i), cassandraClientPool, 1);
        }
    }

    @Test
    public void shouldKeepRetryingIfNowhereToRedirectTo() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = throwingClientPoolWithServersInCurrentPool(
                ImmutableSet.of(host), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(host, input -> null);
            fail();
        } catch (Exception e) {
            // expected, keep going
        }

        verifyNumberOfAttemptsOnHost(host, cassandraClientPool, CassandraClientPool.MAX_TRIES_TOTAL);
    }

    private void verifyNumberOfAttemptsOnHost(InetSocketAddress host,
                                              CassandraClientPool cassandraClientPool,
                                              int numAttempts) {
        Mockito.verify(cassandraClientPool.currentPools.get(host), Mockito.times(numAttempts)).runWithPooledResource(
                Mockito.<FunctionCheckedException<Cassandra.Client, Object, RuntimeException>>any()
        );
    }

    private CassandraClientPool clientPoolWithServers(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, ImmutableSet.of(), Optional.empty());
    }

    private CassandraClientPool clientPoolWithServersInCurrentPool(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(ImmutableSet.of(), servers, Optional.empty());
    }

    private CassandraClientPool throwingClientPoolWithServersInCurrentPool(ImmutableSet<InetSocketAddress> servers,
                                                                           Exception exception) {
        return clientPoolWith(ImmutableSet.of(), servers, Optional.of(exception));
    }

    private CassandraClientPool clientPoolWith(
            ImmutableSet<InetSocketAddress> servers,
            ImmutableSet<InetSocketAddress> serversInPool,
            Optional<Exception> failureMode) {
        CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
        when(config.poolRefreshIntervalSeconds()).thenReturn(POOL_REFRESH_INTERVAL_SECONDS);
        when(config.timeBetweenConnectionEvictionRunsSeconds()).thenReturn(TIME_BETWEEN_EVICTION_RUNS_SECONDS);
        when(config.servers()).thenReturn(servers);

        CassandraClientPool cassandraClientPool = CassandraClientPool.createWithoutChecksForTesting(config);

        cassandraClientPool.currentPools = serversInPool.stream()
                .collect(Collectors.toMap(Function.identity(),
                        address -> getMockPoolingContainerForHost(address, failureMode)));
        return cassandraClientPool;
    }

    private CassandraClientPoolingContainer getMockPoolingContainerForHost(InetSocketAddress address,
                                                                           Optional<Exception> failureMode) {
        CassandraClientPoolingContainer poolingContainer = mock(CassandraClientPoolingContainer.class);
        when(poolingContainer.getHost()).thenReturn(address);
        if (failureMode.isPresent()) {
            try {
                when(poolingContainer.runWithPooledResource(
                        Mockito.<FunctionCheckedException<Cassandra.Client, Object, Exception>>any()))
                        .thenThrow(failureMode.get());
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return poolingContainer;
    }
}
