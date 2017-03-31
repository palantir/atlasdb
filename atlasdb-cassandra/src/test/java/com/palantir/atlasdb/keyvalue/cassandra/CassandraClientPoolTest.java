/*
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.cassandra.thrift.Cassandra;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolTest {
    private static final int POOL_REFRESH_INTERVAL_SECONDS = 3 * 60;
    private static final int TIME_BETWEEN_EVICTION_RUNS_SECONDS = 20;
    private static final int DEFAULT_PORT = 5000;
    private static final int OTHER_PORT = 6000;
    private static final String HOSTNAME_1 = "1.0.0.0";
    private static final String HOSTNAME_2 = "2.0.0.0";
    private static final String HOSTNAME_3 = "3.0.0.0";
    private static final InetSocketAddress HOST_1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
    private static final InetSocketAddress HOST_2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
    private MetricRegistry metricRegistry;

    @Before
    public void setup() {
        AtlasDbMetrics.setMetricRegistry(new MetricRegistry());
        this.metricRegistry = AtlasDbMetrics.getMetricRegistry();
    }

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(HOST_1));
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(HOST_1));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(HOST_1));
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(HOST_1, HOST_2));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost, equalTo(new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT)));
    }


    @Test(expected = UnknownHostException.class)
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, OTHER_PORT);

        CassandraClientPool cassandraClientPool = clientPoolWithServers(ImmutableSet.of(HOST_1, host2));

        cassandraClientPool.getAddressForHost(HOSTNAME_3);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1));

        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> false);
        assertThat(container.isPresent(), is(false));
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));

        int numTrials = 50;
        for (int i = 0; i < numTrials; i++) {
            Optional<CassandraClientPoolingContainer> container
                    = cassandraClientPool.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
            assertThat(container.isPresent(), is(true));
            assertThat(container.get().getHost(), equalTo(HOST_1));
        }
    }

    @Test
    public void shouldNotReturnHostsNotMatchingPredicateEvenWithNodeFailure() {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));

        cassandraClientPool.blacklistedHosts.put(HOST_1, System.currentTimeMillis());
        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> address.equals(HOST_1));
        assertThat(container.isPresent(), is(true));
        assertThat(container.get().getHost(), equalTo(HOST_1));
    }

    @Test
    public void shouldNotAttemptMoreThanOneConnectionOnSuccess() {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1));
        cassandraClientPool.runWithRetryOnHost(HOST_1, input -> null);
        verifyNumberOfAttemptsOnHost(HOST_1, cassandraClientPool, 1);
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
        runNoopOnHostWithRetryWithException(hostList.get(0), cassandraClientPool);

        verifyNumberOfAttemptsOnHost(hostList.get(0), cassandraClientPool, CassandraClientPool.MAX_TRIES_SAME_HOST);
        for (int i = 1; i < numHosts; i++) {
            verifyNumberOfAttemptsOnHost(hostList.get(i), cassandraClientPool, 1);
        }
    }

    @Test
    public void shouldKeepRetryingIfNowhereToRedirectTo() {
        CassandraClientPool cassandraClientPool = throwingClientPoolWithServersInCurrentPool(
                ImmutableSet.of(HOST_1), new SocketTimeoutException());

        runNoopOnHostWithRetryWithException(HOST_1, cassandraClientPool);
        verifyNumberOfAttemptsOnHost(HOST_1, cassandraClientPool, CassandraClientPool.MAX_TRIES_TOTAL);
    }

    @Test
    public void testRequestFailureMetricsWithConnectionException() {
        runTwoNoopsOnTwoHostsAndThrowFromSecondRunOnFirstHost(
                new SocketTimeoutException("test_socket_timeout_exception"));
        verifyAggregateFailureMetrics(0.25, 0.25);
        verifyFailureMetricsOnHost(HOST_1, 0.5, 0.5);
        verifyFailureMetricsOnHost(HOST_2, 0.0, 0.0);
    }

    @Test
    public void testRequestFailureMetricsWithNoConnectionException() {
        runTwoNoopsOnTwoHostsAndThrowFromSecondRunOnFirstHost(
                new NoSuchElementException("test_non_connection_exception"));
        verifyAggregateFailureMetrics(0.25, 0.0);
        verifyFailureMetricsOnHost(HOST_1, 0.5, 0.0);
        verifyFailureMetricsOnHost(HOST_2, 0.0, 0.0);
    }

    private void runTwoNoopsOnTwoHostsAndThrowFromSecondRunOnFirstHost(Exception exception) {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));

        runNoopOnHost(HOST_1, cassandraClientPool);
        runNoopOnHost(HOST_2, cassandraClientPool);
        runNoopOnHost(HOST_2, cassandraClientPool);

        CassandraClientPoolingContainer container = cassandraClientPool.currentPools.get(HOST_1);
        setFailureModeForHost(container, exception);

        runNoopOnHostWithException(HOST_1, cassandraClientPool);
    }

    @Test
    public void testBlacklistMetrics() {
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(HOST_1, HOST_2));
        CassandraClientPoolingContainer container = cassandraClientPool.currentPools.get(HOST_1);
        runNoopWithRetryOnHost(HOST_1, cassandraClientPool);
        verifyBlacklistMetric(0);
        setFailureModeForHost(container, new SocketTimeoutException());
        runNoopWithRetryOnHost(HOST_1, cassandraClientPool);
        verifyBlacklistMetric(1);
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

        serversInPool.forEach(address ->
                cassandraClientPool.addPool(address, getMockPoolingContainerForHost(address, failureMode)));

        return cassandraClientPool;
    }

    private CassandraClientPoolingContainer getMockPoolingContainerForHost(InetSocketAddress address,
                                                                           Optional<Exception> maybeFailureMode) {
        CassandraClientPoolingContainer poolingContainer = mock(CassandraClientPoolingContainer.class);
        when(poolingContainer.getHost()).thenReturn(address);
        if (maybeFailureMode.isPresent()) {
            setFailureModeForHost(poolingContainer, maybeFailureMode.get());
        }
        return poolingContainer;
    }

    private void setFailureModeForHost(CassandraClientPoolingContainer poolingContainer, Exception failureMode) {
        try {
            when(poolingContainer.runWithPooledResource(
                    Mockito.<FunctionCheckedException<Cassandra.Client, Object, Exception>>any()))
                    .thenThrow(failureMode);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private void runNoopOnHost(InetSocketAddress host, CassandraClientPool pool) {
        pool.runOnHost(host, input -> null);
    }

    private void runNoopWithRetryOnHost(InetSocketAddress host, CassandraClientPool pool) {
        pool.runWithRetryOnHost(host, input -> null);
    }

    private void runNoopOnHostWithException(InetSocketAddress host, CassandraClientPool pool) {
        try {
            pool.runOnHost(host, input -> null);
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    private void runNoopOnHostWithRetryWithException(InetSocketAddress host, CassandraClientPool pool) {
        try {
            pool.runWithRetryOnHost(host, input -> null);
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    private void verifyAggregateFailureMetrics(
            double requestFailureProportion,
            double requestConnectionExceptionProportion) {
        assertEquals(
                getAggregateMetricValueForMetricName("requestFailureProportion"),
                requestFailureProportion);
        assertEquals(
                getAggregateMetricValueForMetricName("requestConnectionExceptionProportion"),
                requestConnectionExceptionProportion);
    }

    private void verifyFailureMetricsOnHost(
            InetSocketAddress host,
            double requestFailureProportion,
            double requestConnectionExceptionProportion) {
        assertEquals(
                getMetricValueFromHostAndMetricName(host.getHostString(), "requestFailureProportion"),
                requestFailureProportion);
        assertEquals(
                getMetricValueFromHostAndMetricName(host.getHostString(), "requestConnectionExceptionProportion"),
                requestConnectionExceptionProportion);
    }

    private void verifyBlacklistMetric(Integer expectedSize) {
        assertEquals(getAggregateMetricValueForMetricName("numBlacklistedHosts"), expectedSize);
    }

    private Object getAggregateMetricValueForMetricName(String metricName) {
        String fullyQualifiedMetricName = MetricRegistry.name(CassandraClientPool.class, metricName);
        return metricRegistry.getGauges().get(fullyQualifiedMetricName).getValue();
    }

    private Object getMetricValueFromHostAndMetricName(String hostname, String metricName) {
        String fullyQualifiedMetricName = MetricRegistry.name(CassandraClientPool.class, hostname, metricName);
        return metricRegistry.getGauges().get(fullyQualifiedMetricName).getValue();
    }

}
