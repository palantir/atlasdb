/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;

import javax.naming.LimitExceededException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.qos.QosCassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.qos.QosClient;

public class QosCassandraClientTest {
    private static final ByteBuffer ROW_KEY = ByteBuffer.wrap(PtBytes.toBytes("key"));

    private final CassandraClient mockClient = mock(CassandraClient.class);
    private final QosClient qosClient = mock(QosClient.class);

    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("foo.bar");
    private static final SlicePredicate SLICE_PREDICATE = SlicePredicates.create(SlicePredicates.Range.ALL,
            SlicePredicates.Limit.ONE);

    private CassandraClient client;

    @Before
    public void setUp() {
        client = new QosCassandraClient(mockClient, qosClient);
    }

    @Test
    public void multigetSliceChecksLimit() throws TException, LimitExceededException {
        client.multiget_slice("get", TEST_TABLE, ImmutableList.of(ROW_KEY), SLICE_PREDICATE, ConsistencyLevel.ANY);

        verify(qosClient, times(1)).executeRead(any(), any());
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void batchMutateChecksLimit() throws TException, LimitExceededException {
        client.batch_mutate("put", ImmutableMap.of(), ConsistencyLevel.ANY);

        verify(qosClient, times(1)).executeWrite(any(), any());
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void executeCqlQueryChecksLimit() throws TException, LimitExceededException {
        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat("SELECT * FROM test_table LIMIT 1")
                .build();
        client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ANY);

        verify(qosClient, times(1)).executeRead(any(), any());
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void getRangeSlicesChecksLimit() throws TException, LimitExceededException {
        client.get_range_slices("get", TEST_TABLE, SLICE_PREDICATE, new KeyRange(), ConsistencyLevel.ANY);

        verify(qosClient, times(1)).executeRead(any(), any());
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void casDoesNotCheckLimit() throws TException, LimitExceededException {
        client.cas(TEST_TABLE, ByteBuffer.allocate(1), ImmutableList.of(new Column()), ImmutableList.of(new Column()),
                ConsistencyLevel.SERIAL, ConsistencyLevel.SERIAL);

        verifyNoMoreInteractions(qosClient);
    }
}
