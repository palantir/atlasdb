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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;

public class ProfilingCassandraClientTest {
    private static final CqlQuery CQL_QUERY = CqlQuery.builder()
            .safeQueryFormat("SELECT * FROM atlasdb.foo LIMIT 1;")
            .build();

    private final CassandraClient delegate = mock(CassandraClient.class);
    private final CassandraClient profilingClient = new ProfilingCassandraClient(delegate);

    @After
    public void noMoreInteractions() {
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void passesCqlQueriesAndResultsToAndFromDelegate() throws TException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        CqlResult standardCqlResult = new CqlResult(CqlResultType.ROWS)
                .setRows(ImmutableList.of(new CqlRow(byteBuffer, ImmutableList.of(new Column(byteBuffer)))));
        setDelegateResponseToCqlQuery(standardCqlResult);

        assertThat(executeCqlQuery()).isEqualTo(standardCqlResult);

        verifyCqlQueryWasExecuted();
    }

    @Test
    public void handlesNullCqlResponseFromDelegate() throws TException {
        setDelegateResponseToCqlQuery(null);

        assertThat(executeCqlQuery()).isNull();

        verifyCqlQueryWasExecuted();
    }

    @Test
    public void handlesCqlResponseWithVoidTypeFromDelegate() throws TException {
        setDelegateResponseToCqlQuery(new CqlResult(CqlResultType.VOID));

        assertThat(executeCqlQuery()).isEqualTo(new CqlResult(CqlResultType.VOID));

        verifyCqlQueryWasExecuted();
    }

    @Test
    public void handlesNullMultigetSliceResponseFromDelegate() throws TException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(PtBytes.toBytes("foo"));
        List<ColumnOrSuperColumn> columns =
                ImmutableList.of(new ColumnOrSuperColumn().setColumn(new Column(byteBuffer)));
        ImmutableMap<ByteBuffer, List<ColumnOrSuperColumn>> resultMap = ImmutableMap.of(byteBuffer, columns);

        when(delegate.multiget_slice(any(), any(), any(), any(), any())).thenReturn(resultMap);

        assertThat(delegate.multiget_slice(
                        "getRows",
                        TableReference.createFromFullyQualifiedName("a.b"),
                        ImmutableList.of(byteBuffer),
                        new SlicePredicate(),
                        ConsistencyLevel.QUORUM))
                .containsExactlyInAnyOrderEntriesOf(resultMap);

        verify(delegate)
                .multiget_slice(
                        "getRows",
                        TableReference.createFromFullyQualifiedName("a.b"),
                        ImmutableList.of(byteBuffer),
                        new SlicePredicate(),
                        ConsistencyLevel.QUORUM);
    }

    private void setDelegateResponseToCqlQuery(CqlResult result) throws TException {
        when(delegate.execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.QUORUM))
                .thenReturn(result);
    }

    private CqlResult executeCqlQuery() throws TException {
        return profilingClient.execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.QUORUM);
    }

    private void verifyCqlQueryWasExecuted() throws TException {
        verify(delegate).execute_cql3_query(CQL_QUERY, Compression.NONE, ConsistencyLevel.QUORUM);
    }
}
