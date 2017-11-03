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

import javax.naming.LimitExceededException;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.qos.AtlasDbQosClient;

public class CassandraClientTest {
    private static final ByteBuffer ROW_KEY = ByteBuffer.wrap(PtBytes.toBytes("key"));

    private final Cassandra.Client mockClient = mock(Cassandra.Client.class);
    private final AtlasDbQosClient qosClient = mock(AtlasDbQosClient.class);

    private CassandraClient client;

    @Before
    public void setUp() {
        client = new CassandraClient(mockClient, qosClient);
    }

    @Test
    public void multigetSliceChecksLimit() throws TException, LimitExceededException {
        ColumnParent table = new ColumnParent("table");
        SlicePredicate predicate = SlicePredicates.create(SlicePredicates.Range.ALL, SlicePredicates.Limit.ONE);

        client.multiget_slice(ImmutableList.of(ROW_KEY), table, predicate, ConsistencyLevel.ANY);

        verify(qosClient, times(1)).checkLimit();
        verifyNoMoreInteractions(qosClient);
    }

}
