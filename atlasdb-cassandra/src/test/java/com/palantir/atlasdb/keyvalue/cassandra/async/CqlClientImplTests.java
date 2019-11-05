/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.RowAccumulator;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.StatementPreparer;

@RunWith(MockitoJUnitRunner.class)
public class CqlClientImplTests {
    @Mock
    private CqlSession cqlSession;
    @Mock
    private CqlQuerySpec<Integer> cqlQuerySpec;
    @Mock
    private StatementPreparer statementPreparer;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    Statement statement;
    @Mock
    AsyncResultSet asyncResultSet;
    @Mock
    RowAccumulator<Integer> accumulator;

    private CqlClientImpl cqlClient;

    @Before
    public void setUp() {
        setUpMocks();
        cqlClient = new CqlClientImpl(cqlSession, statementPreparer);
    }

    private void setUpMocks() {
        when(cqlQuerySpec.makeExecutableStatement(preparedStatement)).thenReturn(statement);
        when(statement.setConsistencyLevel(any())).thenReturn(statement);
        when(statementPreparer.prepare(any())).thenReturn(preparedStatement);
        when(cqlSession.executeAsync(statement)).thenReturn(CompletableFuture.completedFuture(asyncResultSet));
        when(cqlQuerySpec.rowAccumulator()).thenReturn(accumulator);
        when(asyncResultSet.currentPage()).thenReturn(null);
    }

    @Test
    public void testOnePageProcessing() {
        when(asyncResultSet.hasMorePages()).thenReturn(false);

        AtlasFutures.getUnchecked(cqlClient.executeQuery(cqlQuerySpec));

        verify(accumulator).accumulateRows(any());
    }

    @Test
    public void testMultiPageProcessing() {
        when(asyncResultSet.hasMorePages()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(asyncResultSet.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet));

        AtlasFutures.getUnchecked(cqlClient.executeQuery(cqlQuerySpec));

        verify(accumulator, times(3)).accumulateRows(any());
    }
}
