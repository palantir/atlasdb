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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.apache.cassandra.thrift.CqlPreparedResult;
import org.apache.cassandra.thrift.CqlResult;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

public class CqlExecutorTest {

    private final CqlExecutorImpl.QueryExecutor queryExecutor = mock(CqlExecutorImpl.QueryExecutor.class);
    private final CqlExecutor executor = new CqlExecutorImpl(queryExecutor);

    private long queryDelayMillis = 0L;

    private static final TableReference TABLE_REF = TableReference.create(Namespace.create("foo"), "bar");
    private static final byte[] ROW = {0x01, 0x02};
    private static final byte[] END_ROW = {0x05, 0x09};
    private static final byte[] COLUMN = {0x03, 0x04};
    private static final long TIMESTAMP = 123L;
    private static final int LIMIT = 100;

    @Before
    public void before() {
        CqlResult result = new CqlResult();
        result.setRows(ImmutableList.of());
        when(queryExecutor.execute(any(), any())).thenAnswer(invocation -> {
            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(queryDelayMillis));
            return result;
        });

        CqlPreparedResult preparedResult = new CqlPreparedResult();
        preparedResult.setItemId(1);
        when(queryExecutor.prepare(any(), any(), any())).thenReturn(preparedResult);

        when(queryExecutor.executePrepared(eq(1), any())).thenReturn(result);
    }

    @Test
    public void getTimestampsForGivenRows() {
        String expected = "SELECT key, column1, column2 FROM \"foo__bar\"" + " WHERE key = ? LIMIT 100;";

        int executorThreads = AtlasDbConstants.DEFAULT_SWEEP_CASSANDRA_READ_THREADS;
        executor.getTimestamps(
                TABLE_REF,
                ImmutableList.of(ROW, END_ROW),
                LIMIT,
                PTExecutors.newFixedThreadPool(executorThreads),
                executorThreads);

        verify(queryExecutor).prepare(argThat(byteBufferMatcher(expected)), eq(ROW), any());
        verify(queryExecutor).executePrepared(eq(1), eq(ImmutableList.of(ByteBuffer.wrap(ROW))));
        verify(queryExecutor).executePrepared(eq(1), eq(ImmutableList.of(ByteBuffer.wrap(END_ROW))));
    }

    @Test
    public void getTimestampsWithinRow() {
        String expected = "SELECT column1, column2 FROM \"foo__bar\" WHERE key = 0x0102"
                + " AND (column1, column2) > (0x0304, -124) LIMIT 100;";

        executor.getTimestampsWithinRow(TABLE_REF, ROW, COLUMN, TIMESTAMP, LIMIT);

        verify(queryExecutor).execute(argThat(cqlQueryMatcher(expected)), eq(ROW));
    }

    private ArgumentMatcher<ByteBuffer> byteBufferMatcher(String expected) {
        return argument -> {
            if (argument == null) {
                return false;
            }

            int position = argument.position();
            byte[] data = new byte[argument.remaining()];
            argument.get(data);
            argument.position(position);
            String actualQuery = PtBytes.toString(data);
            return expected.equals(actualQuery);
        };
    }

    private ArgumentMatcher<CqlQuery> cqlQueryMatcher(String expected) {
        return argument -> {
            if (argument == null) {
                return false;
            }

            return expected.equals(argument.toString());
        };
    }
}
