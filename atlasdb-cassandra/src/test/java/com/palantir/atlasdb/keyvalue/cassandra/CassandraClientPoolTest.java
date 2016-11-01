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
import static org.junit.Assume.assumeTrue;
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
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;

public class CassandraClientPoolTest {
    public static final int POOL_REFRESH_INTERVAL_SECONDS = 10;
    public static final int DEFAULT_PORT = 5000;
    public static final int OTHER_PORT = 6000;
    public static final String HOSTNAME_1 = "1.0.0.0";
    public static final String HOSTNAME_2 = "2.0.0.0";
    public static final String HOSTNAME_3 = "3.0.0.0";

    public static final int FUZZING_NUM_TRIALS = 20;

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

        for (int i = 0; i < FUZZING_NUM_TRIALS; i++) {
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
    public void shouldRedirectToPreferredHostsIfAvailable() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        for (int i = 0; i < FUZZING_NUM_TRIALS; i++) {
            CassandraClientPoolingContainer container
                    = cassandraClientPool.getRedirectTarget(ImmutableSet.of(), ImmutableSet.of(host1));
            assertThat(container.getHost(), equalTo(host1));
        }
    }

    @Test
    public void shouldRedirectToOtherPreferredHostAfterTryingOne() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        InetSocketAddress host3 = new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool
                = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2, host3));

        for (int i = 0; i < FUZZING_NUM_TRIALS; i++) {
            CassandraClientPoolingContainer container
                    = cassandraClientPool.getRedirectTarget(ImmutableSet.of(host1), ImmutableSet.of(host1, host2));
            assertThat(container.getHost(), equalTo(host2));
        }
    }

    @Test
    public void shouldRedirectToNonPreferredHostsAfterTryingAllPreferred() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        for (int i = 0; i < FUZZING_NUM_TRIALS; i++) {
            CassandraClientPoolingContainer container
                    = cassandraClientPool.getRedirectTarget(ImmutableSet.of(host1), ImmutableSet.of(host1));
            assertThat(container.getHost(), equalTo(host2));
        }
    }

    @Test
    public void shouldBeAbleToRedirectAfterTryingAllHosts() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        cassandraClientPool.getRedirectTarget(ImmutableSet.of(host1, host2), ImmutableSet.of(host1));
    }

    @Test
    public void shouldBeResilientToNonexistentPreferredHosts() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress fakePreferred = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        CassandraClientPoolingContainer container =
                cassandraClientPool.getRedirectTarget(ImmutableSet.of(), ImmutableSet.of(fakePreferred));
        assertThat(container.getHost(), equalTo(host));
    }

    @Test
    public void shouldNotAttemptMoreThanOneConnectionOnSuccess() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = clientPoolWithServersInCurrentPool(ImmutableSet.of(host));
        cassandraClientPool.runWithRetryOnHost(host, input -> null);
        verifyNumberOfAttemptsOnHost(cassandraClientPool, host, 1);
    }

    @Test
    public void shouldRetryOnSameNodeToFailureAndThenRedirect() {
        int numHosts = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;
        assumeTrue(numHosts > 1);
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

        verifyNumberOfAttemptsOnHost(cassandraClientPool, hostList.get(0), CassandraClientPool.MAX_TRIES_SAME_HOST);
        for (int i = 1; i < numHosts; i++) {
            verifyNumberOfAttemptsOnHost(cassandraClientPool, hostList.get(i), 1);
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

        verifyNumberOfAttemptsOnHost(cassandraClientPool, host, CassandraClientPool.MAX_TRIES_TOTAL);
    }

    @Test
    public void shouldFollowHostPreferencesWhenRetrying() {
        // We want to verify the query touches every host. We try MAX_TRIES_SAME_HOST times on the first host, and
        // then try to contact the other hosts once each.
        int numHosts = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;
        assumeTrue(numHosts > 1);
        List<InetSocketAddress> hostList = Lists.newArrayList();
        for (int i = 0; i < numHosts; i++) {
            hostList.add(new InetSocketAddress(i));
        }

        CassandraClientPool cassandraClientPool = throwingClientPoolWithServersInCurrentPool(
                ImmutableSet.copyOf(hostList), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(hostList.get(0), input -> null, ImmutableSet.of(hostList.get(1)));
        } catch (Exception e) {
            // expected, keep going
        }

        // Verify we tried host 0 before host 1. This needs to be here in case numHosts == 2
        verifySequenceOfHostAttempts(cassandraClientPool, ImmutableList.of(hostList.get(0), hostList.get(1)));
        for (int i = 2; i < numHosts; i++) {
            // Verify we tried host 0, then host 1, then host i
            verifySequenceOfHostAttempts(cassandraClientPool, ImmutableList.of(
                    hostList.get(0), hostList.get(1), hostList.get(i)
            ));
        }
    }

    @Test
    public void shouldAttemptNonPreferredHostAfterTryingAllPreferredHosts() {
        // This test insists on there being a non-hinted server, which shouldFollowHintsWhenRetrying() does not.
        assumeTrue(CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST >= 2);
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        InetSocketAddress host3 = new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool = throwingClientPoolWithServersInCurrentPool(
                ImmutableSet.of(host1, host2, host3), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(host1, input -> null, ImmutableSet.of(host2));
        } catch (Exception e) {
            // expected, keep going
        }

        verifyHostAttempted(cassandraClientPool, host3);
    }

    private void verifySequenceOfHostAttempts(CassandraClientPool cassandraClientPool,
            List<InetSocketAddress> ordering) {
        List<CassandraClientPoolingContainer> poolingContainers = ordering.stream()
                .map(cassandraClientPool.currentPools::get)
                .collect(Collectors.toList());
        InOrder inOrder = Mockito.inOrder(poolingContainers.toArray());

        for (CassandraClientPoolingContainer container : poolingContainers) {
            inOrder.verify(container, Mockito.atLeastOnce()).runWithPooledResource(
                    Mockito.<FunctionCheckedException<Cassandra.Client, Object, RuntimeException>>any()
            );
        }
    }

    private void verifyHostAttempted(CassandraClientPool cassandraClientPool,
                                     InetSocketAddress host) {
        verifyAttemptsOnHost(cassandraClientPool, host, Mockito.atLeastOnce());
    }

    private void verifyNumberOfAttemptsOnHost(CassandraClientPool cassandraClientPool,
                                              InetSocketAddress host,
                                              int numAttempts) {
        verifyAttemptsOnHost(cassandraClientPool, host, Mockito.times(numAttempts));
    }

    private void verifyAttemptsOnHost(CassandraClientPool cassandraClientPool,
                                      InetSocketAddress host,
                                      VerificationMode verificationMode) {
        Mockito.verify(cassandraClientPool.currentPools.get(host), verificationMode).runWithPooledResource(
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
        when(config.servers()).thenReturn(servers);

        CassandraClientPool cassandraClientPool = new CassandraClientPool(config);
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
