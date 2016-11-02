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

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Throwables;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;

public class CqlExecutorTest {

    private static final byte[] KEY_1 = { 0, 0, 0 };
    private static final byte[] KEY_2 = { 1, 1, 0 };

    private static final Range<CassandraClientPool.LightweightOppToken> DEFAULT_RANGE =
            Range.range(new CassandraClientPool.LightweightOppToken(KEY_1), BoundType.CLOSED,
                    new CassandraClientPool.LightweightOppToken(KEY_2), BoundType.OPEN);

    private static final TableReference DEFAULT_TABLE_REFERENCE = TableReference.createWithEmptyNamespace("foo");
    private static final int DEFAULT_ROW_LIMIT = 5;

    @Test
    public void shouldDirectQueriesToServerWithRelevantRowKey() {
        InetSocketAddress host1 = new InetSocketAddress(1);
        InetSocketAddress host2 = new InetSocketAddress(2);
        InetSocketAddress host3 = new InetSocketAddress(3);

        CassandraClientPool cassandraClientPool =
                setupMockCassandraClientPoolWithCqlResponses(ImmutableSet.of(host1, host2, host3));
        cassandraClientPool.tokenMap = ImmutableRangeMap.of(DEFAULT_RANGE, ImmutableList.of(host1));

        CqlExecutor cqlExecutor = new CqlExecutor(cassandraClientPool, ConsistencyLevel.QUORUM);
        cqlExecutor.getColumnsForRow(DEFAULT_TABLE_REFERENCE, KEY_1, DEFAULT_ROW_LIMIT);

        MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(cassandraClientPool, host1, 1);
    }

    @Test
    public void shouldNotThrowIfTokenMapUnknown() {
        InetSocketAddress host = new InetSocketAddress(1);

        CassandraClientPool cassandraClientPool =
                setupMockCassandraClientPoolWithCqlResponses(ImmutableSet.of(host));

        CqlExecutor cqlExecutor = new CqlExecutor(cassandraClientPool, ConsistencyLevel.QUORUM);
        cqlExecutor.getColumnsForRow(DEFAULT_TABLE_REFERENCE, KEY_1, DEFAULT_ROW_LIMIT);
    }

    @Test
    public void shouldPreferentiallyRetryOnServersWithRelevantRowKey() {
        int numHostsWithData = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST + 1;

        List<InetSocketAddress> dataHosts = Lists.newArrayList();
        for (int i = 0; i < numHostsWithData; i++) {
            dataHosts.add(new InetSocketAddress(i));
        }
        InetSocketAddress nonDataHost = new InetSocketAddress(numHostsWithData);

        CassandraClientPool cassandraClientPool = setupMockCassandraClientPoolForConfig(dataHosts, nonDataHost);

        CqlExecutor cqlExecutor = new CqlExecutor(cassandraClientPool, ConsistencyLevel.QUORUM);
        try {
            cqlExecutor.getColumnsForRow(DEFAULT_TABLE_REFERENCE, KEY_1, DEFAULT_ROW_LIMIT);
            fail();
        } catch (Exception exception) {
            // expected
        }

        MockCassandraClientPoolUtils.verifyNumberOfAttemptsOnHost(cassandraClientPool, nonDataHost, 0);
    }

    @Test
    public void shouldRetryOnServersWithoutRowKeyAfterExhaustion() {
        int numHostsWithData = CassandraClientPool.MAX_TRIES_TOTAL - CassandraClientPool.MAX_TRIES_SAME_HOST;

        List<InetSocketAddress> dataHosts = Lists.newArrayList();
        for (int i = 0; i < numHostsWithData; i++) {
            dataHosts.add(new InetSocketAddress(i));
        }
        InetSocketAddress nonDataHost = new InetSocketAddress(numHostsWithData);

        CassandraClientPool cassandraClientPool = setupMockCassandraClientPoolForConfig(dataHosts, nonDataHost);

        CqlExecutor cqlExecutor = new CqlExecutor(cassandraClientPool, ConsistencyLevel.QUORUM);
        try {
            cqlExecutor.getColumnsForRow(DEFAULT_TABLE_REFERENCE, KEY_1, DEFAULT_ROW_LIMIT);
            fail();
        } catch (Exception exception) {
            // expected
        }

        dataHosts.forEach(dataHost ->
                MockCassandraClientPoolUtils.verifySequenceOfHostAttempts(
                        cassandraClientPool, ImmutableList.of(dataHost, nonDataHost)));
    }

    private CassandraClientPool setupMockCassandraClientPoolForConfig(
            List<InetSocketAddress> dataHosts,
            InetSocketAddress nonDataHost) {
        List<InetSocketAddress> allHosts = Lists.newArrayList(dataHosts);
        allHosts.add(nonDataHost);

        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.throwingClientPoolWithServersInCurrentPool(
                        ImmutableSet.copyOf(allHosts),
                        new SocketTimeoutException());
        cassandraClientPool.tokenMap = ImmutableRangeMap.of(DEFAULT_RANGE, ImmutableList.copyOf(dataHosts));
        return cassandraClientPool;
    }

    private CassandraClientPool setupMockCassandraClientPoolWithCqlResponses(ImmutableSet<InetSocketAddress> hosts) {
        CassandraClientPool cassandraClientPool =
                MockCassandraClientPoolUtils.clientPoolWithServersInCurrentPool(hosts);

        cassandraClientPool.currentPools.values().forEach(this::setupPoolingContainerToReturnEmptyCqlResult);
        return cassandraClientPool;
    }

    private void setupPoolingContainerToReturnEmptyCqlResult(CassandraClientPoolingContainer poolingContainer) {
        try {
            CqlResult cqlResult = new CqlResult(CqlResultType.ROWS);
            cqlResult.setRows(ImmutableList.of());
            Mockito.when(poolingContainer.runWithPooledResource(
                    Mockito.<FunctionCheckedException<Cassandra.Client, CqlResult, Exception>>any()))
                    .thenReturn(cqlResult);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
