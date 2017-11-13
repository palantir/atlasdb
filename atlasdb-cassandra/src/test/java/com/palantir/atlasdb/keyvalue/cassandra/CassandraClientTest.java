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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import javax.naming.LimitExceededException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
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
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.qos.QosClient;

public class CassandraClientTest {
    private static final ByteBuffer ROW_KEY = ByteBuffer.wrap(PtBytes.toBytes("key"));

    private final Cassandra.Client mockClient = mock(Cassandra.Client.class);
    private final QosClient qosClient = mock(QosClient.class);

    private static final ColumnParent TEST_TABLE = new ColumnParent("table");
    private static final SlicePredicate SLICE_PREDICATE = SlicePredicates.create(SlicePredicates.Range.ALL,
            SlicePredicates.Limit.ONE);

    private CassandraClient client;

    @Before
    public void setUp() {
        client = new CassandraClient(mockClient, qosClient);
    }

    @Test
    public void multigetSliceChecksLimit() throws TException, LimitExceededException {
        client.multiget_slice(ImmutableList.of(ROW_KEY), TEST_TABLE, SLICE_PREDICATE, ConsistencyLevel.ANY);

        verify(qosClient, times(1)).checkLimit();
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void batchMutateChecksLimit() throws TException, LimitExceededException {
        client.batch_mutate(ImmutableMap.of(), ConsistencyLevel.ANY);

        verify(qosClient, times(1)).checkLimit();
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void executeCqlQueryChecksLimit() throws TException, LimitExceededException {
        ByteBuffer query = ByteBuffer.wrap("SELECT * FROM test_table LIMIT 1".getBytes(StandardCharsets.UTF_8));
        client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.ANY);

        verify(qosClient, times(1)).checkLimit();
        verifyNoMoreInteractions(qosClient);
    }

    @Test
    public void getRangeSlicesChecksLimit() throws TException, LimitExceededException {
        client.get_range_slices(TEST_TABLE, SLICE_PREDICATE, new KeyRange(), ConsistencyLevel.ANY);

        verify(qosClient, times(1)).checkLimit();
        verifyNoMoreInteractions(qosClient);
    }
}
