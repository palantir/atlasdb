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

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class CassandraClientPoolTest {
    public static final int DEFAULT_PORT = 5000;
    public static final int OTHER_PORT = 6000;
    public static final String HOSTNAME_1 = "1.0.0.0";
    public static final String HOSTNAME_2 = "2.0.0.0";
    public static final String HOSTNAME_3 = "3.0.0.0";

    public static final int FUZZING_NUM_TRIALS = 20;

    private static final byte[] KEY_1 = { 1, 2, 3 };
    private static final byte[] KEY_2 = { 1, 3, 5 };
    private static final byte[] KEY_3 = { 2, 1, 1 };

    @Test
    public void shouldReturnAddressForSingleHostInPool() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldReturnAddressForSingleServer() throws UnknownHostException {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServers(ImmutableSet.of(host));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_1);

        assertThat(resolvedHost, equalTo(host));
    }

    @Test
    public void shouldUseCommonPortIfThereIsOnlyOneAndNoAddressMatches() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServers(ImmutableSet.of(host1, host2));

        InetSocketAddress resolvedHost = cassandraClientPool.getAddressForHost(HOSTNAME_3);

        assertThat(resolvedHost, equalTo(new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT)));
    }


    @Test(expected = UnknownHostException.class)
    public void shouldThrowIfPortsAreNotTheSameAddressDoesNotMatch() throws UnknownHostException {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, OTHER_PORT);

        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServers(ImmutableSet.of(host1, host2));

        cassandraClientPool.getAddressForHost(HOSTNAME_3);
    }

    @Test
    public void shouldReturnAbsentIfPredicateMatchesNoServers() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> false);
        assertThat(container.isPresent(), is(false));
    }

    @Test
    public void shouldOnlyReturnHostsMatchingPredicate() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

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
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        cassandraClientPool.blacklistedHosts.put(host1, System.currentTimeMillis());
        Optional<CassandraClientPoolingContainer> container
                = cassandraClientPool.getRandomGoodHostForPredicate(address -> address.equals(host1));
        assertThat(container.isPresent(), is(true));
        assertThat(container.get().getHost(), equalTo(host1));
    }

    @Test
    public void canGetUntriedPreferredHosts() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        assertThat(cassandraClientPool.getRandomUntriedPreferredHost(
                ImmutableSet.of(), ImmutableSet.of(host1)).get().getHost(), is(host1));
        assertThat(cassandraClientPool.getRandomUntriedPreferredHost(
                ImmutableSet.of(host1), ImmutableSet.of(host1, host2)).get().getHost(), is(host2));
        assertThat(cassandraClientPool.getRandomUntriedPreferredHost(
                ImmutableSet.of(host1, host2), ImmutableSet.of(host1, host2)).isPresent(), is(false));
    }

    @Test
    public void getUntriedPreferredHostsReturnsAbsentIfAllPreferredHostsTried() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        assertThat(cassandraClientPool.getRandomUntriedPreferredHost(
                ImmutableSet.of(host1, host2), ImmutableSet.of(host1, host2)).isPresent(), is(false));
        assertThat(cassandraClientPool.getRandomUntriedPreferredHost(
                ImmutableSet.of(host1, host2), ImmutableSet.of(host1)).isPresent(), is(false));
    }

    @Test
    public void canGetUntriedHosts() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        assertThat(cassandraClientPool.getRandomUntriedHost(ImmutableSet.of(host1)).get().getHost(),
                is(host2));
        assertThat(cassandraClientPool.getRandomUntriedHost(ImmutableSet.of(host2)).get().getHost(),
                is(host1));
    }

    @Test
    public void getUntriedHostsReturnsAbsentIfAllHostsTried() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        assertThat(cassandraClientPool.getRandomUntriedHost(ImmutableSet.of(host1, host2)).isPresent(),
                is(false));
    }

    @Test
    public void shouldRedirectToPreferredHostsIfAvailable() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

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
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2, host3));

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
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

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
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        cassandraClientPool.getRedirectTarget(ImmutableSet.of(host1, host2), ImmutableSet.of(host1));
    }

    @Test
    public void shouldBeResilientToNonexistentPreferredHosts() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress fakePreferred = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host));

        CassandraClientPoolingContainer container =
                cassandraClientPool.getRedirectTarget(ImmutableSet.of(), ImmutableSet.of(fakePreferred));
        assertThat(container.getHost(), equalTo(host));
    }

    @Test
    public void shouldBeAbleToGetHostsForKey() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));
        List<InetSocketAddress> hostList = Lists.newArrayList(host1);
        cassandraClientPool.tokenMap = ImmutableRangeMap.of(
                Range.range(new CassandraClientPool.LightweightOppToken(KEY_1), BoundType.CLOSED,
                            new CassandraClientPool.LightweightOppToken(KEY_3), BoundType.OPEN),
                hostList);

        assertThat(cassandraClientPool.getHostsForKey(KEY_1).get(), is(hostList));
        assertThat(cassandraClientPool.getHostsForKey(KEY_2).get(), is(hostList));
        assertThat(cassandraClientPool.getHostsForKey(KEY_3).isPresent(), is(false));
    }

    @Test
    public void shouldReturnAbsentIfTokenMapNotInitialised() {
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host1, host2));

        assertThat(cassandraClientPool.getHostsForKey(KEY_1).isPresent(), is(false));
    }

    @Test
    public void shouldNotAttemptMoreThanOneConnectionOnSuccess() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(ImmutableSet.of(host));
        cassandraClientPool.runWithRetryOnHost(host, input -> null);
        MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(cassandraClientPool, host, 1);
    }

    @Test
    public void shouldRetryOnSameNodeToFailureAndThenRedirect() {
        int numHosts = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;
        assumeTrue(numHosts > 1);
        List<InetSocketAddress> hostList = Lists.newArrayList();
        for (int i = 0; i < numHosts; i++) {
            hostList.add(new InetSocketAddress(i));
        }

        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.throwingClientPoolWithServersInCurrentPool(
                        ImmutableSet.copyOf(hostList), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(hostList.get(0), input -> null);
            fail();
        } catch (Exception e) {
            // expected, keep going
        }

        MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(
                cassandraClientPool,
                hostList.get(0),
                CassandraClientPool.MAX_TRIES_SAME_HOST);
        for (int i = 1; i < numHosts; i++) {
            MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(cassandraClientPool, hostList.get(i), 1);
        }
    }

    @Test
    public void shouldKeepRetryingIfNowhereToRedirectTo() {
        InetSocketAddress host = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.throwingClientPoolWithServersInCurrentPool(
                        ImmutableSet.of(host), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(host, input -> null);
            fail();
        } catch (Exception e) {
            // expected, keep going
        }

        MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(cassandraClientPool, host,
                CassandraClientPool.MAX_TRIES_TOTAL);
    }

    @Test
    public void shouldFollowHostPreferencesWhenRetrying() {
        int numHosts = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;
        assumeTrue(numHosts > 1);
        List<InetSocketAddress> hostList = Lists.newArrayList();
        for (int i = 0; i < numHosts; i++) {
            hostList.add(new InetSocketAddress(i));
        }

        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.throwingClientPoolWithServersInCurrentPool(
                        ImmutableSet.copyOf(hostList), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(hostList.get(0), input -> null, ImmutableSet.of(hostList.get(1)));
        } catch (Exception e) {
            // expected, keep going
        }

        MockCassandraClientPoolUtils.verifySequenceOfHostAttempts(
                cassandraClientPool,
                ImmutableList.of(hostList.get(0), hostList.get(1)));
        for (int i = 2; i < numHosts; i++) {
            MockCassandraClientPoolUtils.verifySequenceOfHostAttempts(
                    cassandraClientPool,
                    ImmutableList.of(hostList.get(0), hostList.get(1), hostList.get(i)));
        }
    }

    @Test
    public void shouldAttemptNonPreferredHostAfterTryingAllPreferredHosts() {
        assumeTrue(CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST >= 2);
        InetSocketAddress host1 = new InetSocketAddress(HOSTNAME_1, DEFAULT_PORT);
        InetSocketAddress host2 = new InetSocketAddress(HOSTNAME_2, DEFAULT_PORT);
        InetSocketAddress host3 = new InetSocketAddress(HOSTNAME_3, DEFAULT_PORT);
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.throwingClientPoolWithServersInCurrentPool(
                        ImmutableSet.of(host1, host2, host3), new SocketTimeoutException());

        try {
            cassandraClientPool.runWithRetryOnHost(host1, input -> null, ImmutableSet.of(host2));
        } catch (Exception e) {
            // expected, keep going
        }

        MockCassandraClientPoolUtils.verifyHostAttempted(cassandraClientPool, host3);
    }
}
