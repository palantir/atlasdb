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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CqlQuery;
import com.palantir.atlasdb.keyvalue.cassandra.QosCassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.QueryWeight;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.ThriftQueryWeighers;
import com.palantir.atlasdb.qos.metrics.QosMetrics;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import javax.naming.LimitExceededException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

public class QosCassandraClientTest {
    private final CassandraClient mockClient = mock(CassandraClient.class);
    private final QosMetrics mockMetrics = mock(QosMetrics.class);
    private final Ticker ticker = mock(Ticker.class);

    private static final long NANOS_START = 1L;
    private static final long NANOS_END = 13L;
    private static final long NANOS_DURATION = NANOS_END - NANOS_START;

    private static final ByteBuffer ROW_KEY = ByteBuffer.wrap(PtBytes.toBytes("key"));
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final SlicePredicate SLICE_PREDICATE = SlicePredicates.create(SlicePredicates.Range.ALL,
            SlicePredicates.Limit.ONE);
    private static final Map<ByteBuffer, List<ColumnOrSuperColumn>> MULTIGET_RESULT = ImmutableMap.of(ROW_KEY,
            ImmutableList.of(new ColumnOrSuperColumn()));
    private static final ImmutableMap<ByteBuffer, Map<String, List<Mutation>>> BATCH_MUTATE_ARG = ImmutableMap.of(
            ROW_KEY, ImmutableMap.of());
    private static final CqlQuery CQL_QUERY = CqlQuery.builder()
            .safeQueryFormat("SELECT * FROM test_table LIMIT 1")
            .build();
    private static final KeyRange KEY_RANGE = new KeyRange();

    private CassandraClient client;

    @Before
    public void setUp() {
        client = new QosCassandraClient(mockClient, mockMetrics, ticker);
        when(ticker.read()).thenReturn(NANOS_START).thenReturn(NANOS_END);
    }

    @Test
    public void multigetSliceRecordsMetricsOnSuccess() throws TException, LimitExceededException {
        when(mockClient.multiget_slice(any(), any(), any(), any(), any())).thenReturn(MULTIGET_RESULT);
        QueryWeight expectedWeight = ThriftQueryWeighers
                .multigetSlice(ImmutableList.of(ROW_KEY))
                .weighSuccess(MULTIGET_RESULT, NANOS_DURATION);

        client.multiget_slice("get", TEST_TABLE, ImmutableList.of(ROW_KEY), SLICE_PREDICATE, ConsistencyLevel.ANY);

        verify(mockClient, times(1)).multiget_slice("get", TEST_TABLE, ImmutableList.of(ROW_KEY), SLICE_PREDICATE,
                ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void multigetSliceRecordsMetricsOnFailure() throws TException, LimitExceededException {
        when(mockClient.multiget_slice(any(), any(), any(), any(), any())).thenThrow(new RuntimeException());
        QueryWeight expectedWeight = ThriftQueryWeighers
                .multigetSlice(ImmutableList.of(ROW_KEY))
                .weighFailure(new RuntimeException(), NANOS_DURATION);

        assertThatThrownBy(() -> client
                .multiget_slice("get", TEST_TABLE, ImmutableList.of(ROW_KEY), SLICE_PREDICATE, ConsistencyLevel.ANY))
                .isInstanceOf(RuntimeException.class);

        verify(mockClient, times(1)).multiget_slice("get", TEST_TABLE, ImmutableList.of(ROW_KEY), SLICE_PREDICATE,
                ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void batchMutateRecordsMetricsOnSuccess() throws TException, LimitExceededException {
        QueryWeight expectedWeight = ThriftQueryWeighers
                .batchMutate(BATCH_MUTATE_ARG)
                .weighSuccess(null, NANOS_DURATION);

        client.batch_mutate("put", BATCH_MUTATE_ARG, ConsistencyLevel.ANY);

        verify(mockClient, times(1)).batch_mutate("put", BATCH_MUTATE_ARG, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordWrite(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void batchMutateRecordsMetricsOnFailure() throws TException, LimitExceededException {
        doThrow(new RuntimeException()).when(mockClient).batch_mutate(any(), any(), any());
        QueryWeight expectedWeight = ThriftQueryWeighers
                .batchMutate(BATCH_MUTATE_ARG)
                .weighSuccess(null, NANOS_DURATION);

        assertThatThrownBy(() -> client.batch_mutate("put", BATCH_MUTATE_ARG, ConsistencyLevel.ANY))
                .isInstanceOf(RuntimeException.class);

        verify(mockClient, times(1)).batch_mutate("put", BATCH_MUTATE_ARG, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordWrite(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void executeCqlQueryRecordsMetricsOnSuccess() throws TException, LimitExceededException {
        CqlResult emptyResult = new CqlResult();
        when(mockClient.execute_cql3_query(any(), any(), any())).thenReturn(emptyResult);
        QueryWeight expectedWeight = ThriftQueryWeighers.EXECUTE_CQL3_QUERY.weighSuccess(emptyResult, NANOS_DURATION);

        client.execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.ANY);

        verify(mockClient, times(1)).execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void executeCqlQueryRecordsMetricsOnFailure() throws TException, LimitExceededException {
        when(mockClient.execute_cql3_query(any(), any(), any())).thenThrow(new RuntimeException());
        QueryWeight expectedWeight = ThriftQueryWeighers.EXECUTE_CQL3_QUERY
                .weighFailure(new RuntimeException(), NANOS_DURATION);

        assertThatThrownBy(() -> client.execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.ANY))
                .isInstanceOf(RuntimeException.class);

        verify(mockClient, times(1)).execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void getRangeSlicesRecordsMetricsOnSuccess() throws TException, LimitExceededException {
        List<KeySlice> keySlices = ImmutableList.of();
        when(mockClient.get_range_slices(any(), any(), any(), any(), any())).thenReturn(keySlices);
        QueryWeight expectedWeight = ThriftQueryWeighers.getRangeSlices(KEY_RANGE)
                .weighSuccess(keySlices, NANOS_DURATION);

        client.get_range_slices("get", TEST_TABLE, SLICE_PREDICATE, KEY_RANGE, ConsistencyLevel.ANY);

        verify(mockClient, times(1))
                .get_range_slices("get", TEST_TABLE, SLICE_PREDICATE, KEY_RANGE, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void getRangeSlicesRecordsMetricsOnFailure() throws TException, LimitExceededException {
        when(mockClient.get_range_slices(any(), any(), any(), any(), any())).thenThrow(new RuntimeException());
        QueryWeight expectedWeight = ThriftQueryWeighers.getRangeSlices(KEY_RANGE)
                .weighFailure(new RuntimeException(), NANOS_DURATION);

        assertThatThrownBy(() ->
                client.get_range_slices("get", TEST_TABLE, SLICE_PREDICATE, KEY_RANGE, ConsistencyLevel.ANY))
                .isInstanceOf(RuntimeException.class);

        verify(mockClient, times(1))
                .get_range_slices("get", TEST_TABLE, SLICE_PREDICATE, KEY_RANGE, ConsistencyLevel.ANY);
        verifyNoMoreInteractions(mockClient);

        verify(mockMetrics, times(1)).recordRead(expectedWeight);
        verifyNoMoreInteractions(mockMetrics);
    }

    @Test
    public void casDoesNotUpdateMetrics() throws TException, LimitExceededException {
        client.cas(TEST_TABLE, ByteBuffer.allocate(1), ImmutableList.of(new Column()), ImmutableList.of(new Column()),
                ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL);

        verify(mockClient, times(1)).cas(TEST_TABLE, ByteBuffer.allocate(1), ImmutableList.of(new Column()),
                ImmutableList.of(new Column()), ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL);
        verifyNoMoreInteractions(mockClient);

        verifyNoMoreInteractions(mockMetrics);
    }
}
