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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;

public final class MockCassandraClientPoolUtils {

    public static final int POOL_REFRESH_INTERVAL_SECONDS = 10;

    private MockCassandraClientPoolUtils() {
        // utilities
    }

    public static void verifyHostAttempted(CassandraClientPool cassandraClientPool, InetSocketAddress host) {
        verifyAttemptsOnHost(cassandraClientPool, host, Mockito.atLeastOnce());
    }

    public static void verifyNumberOfAttemptsOnHost(
            CassandraClientPool cassandraClientPool,
            InetSocketAddress host,
            int numAttempts) {
        verifyAttemptsOnHost(cassandraClientPool, host, Mockito.times(numAttempts));
    }

    private static void verifyAttemptsOnHost(
            CassandraClientPool cassandraClientPool,
            InetSocketAddress host,
            VerificationMode verificationMode) {
        Mockito.verify(cassandraClientPool.currentPools.get(host), verificationMode).runWithPooledResource(
                Mockito.<FunctionCheckedException<Cassandra.Client, Object, RuntimeException>>any()
        );
    }

    public static void verifySequenceOfHostAttempts(
            CassandraClientPool cassandraClientPool,
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


    public static CassandraClientPool clientPoolWithServers(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(servers, ImmutableSet.of(), Optional.empty());
    }

    public static CassandraClientPool clientPoolWithServersInCurrentPool(ImmutableSet<InetSocketAddress> servers) {
        return clientPoolWith(ImmutableSet.of(), servers, Optional.empty());
    }

    public static CassandraClientPool throwingClientPoolWithServersInCurrentPool(
            ImmutableSet<InetSocketAddress> servers,
            Exception exception) {
        return clientPoolWith(ImmutableSet.of(), servers, Optional.of(exception));
    }

    private static CassandraClientPool clientPoolWith(
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

    private static CassandraClientPoolingContainer getMockPoolingContainerForHost(
            InetSocketAddress address,
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
