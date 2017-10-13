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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.CqlResult;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.KvsProfilingLogger;

public class CqlExecutorTest {

    private final CqlExecutor.QueryExecutor queryExecutor = mock(CqlExecutor.QueryExecutor.class);
    private final CqlExecutor executor = new CqlExecutor(queryExecutor);

    private long queryDelayMillis = 0L;

    private static final TableReference TABLE_REF = TableReference.create(Namespace.create("foo"), "bar");
    private static final byte[] ROW = {0x01, 0x02};
    private static final byte[] COLUMN = {0x03, 0x04};
    private static final long TIMESTAMP = 123L;
    private static final int LIMIT = 100;

    @Before
    public void before() {
        CqlResult result = new CqlResult();
        result.setRows(ImmutableList.of());
        when(queryExecutor.execute(any(), any())).thenAnswer(invocation -> {
            Uninterruptibles.sleepUninterruptibly(queryDelayMillis, TimeUnit.MILLISECONDS);
            return result;
        });
    }

    @Test
    public void getColumnsForRow() {
        String expected = "SELECT column1, column2 FROM \"foo__bar\" WHERE key = 0x0102 LIMIT 100;";

        executor.getColumnsForRow(TABLE_REF, ROW, LIMIT);

        verify(queryExecutor).execute(ROW, expected);
    }

    @Test
    public void getTimestampsForRowAndColumn() {
        String expected = "SELECT column1, column2 FROM \"foo__bar\" WHERE key = 0x0102 AND column1 = 0x0304 "
                + "AND column2 > -124 LIMIT 100;";

        executor.getTimestampsForRowAndColumn(TABLE_REF, ROW, COLUMN, TIMESTAMP, LIMIT);

        verify(queryExecutor).execute(ROW, expected);
    }

    @Test
    public void getNextColumnsForRow() {
        String expected = "SELECT column1, column2 FROM \"foo__bar\" WHERE key = 0x0102 AND column1 > 0x0304 "
                + "LIMIT 100;";

        executor.getNextColumnsForRow(TABLE_REF, ROW, COLUMN, LIMIT);

        verify(queryExecutor).execute(ROW, expected);
    }

    // this test just verifies that nothing blows up when logging a slow query, and the output can be verified manually
    @Test
    public void logsSlowResult() {
        queryDelayMillis = 10;
        KvsProfilingLogger.setSlowLogThresholdMillis(1);

        executor.getColumnsForRow(TABLE_REF, ROW, LIMIT);
        executor.getTimestampsForRowAndColumn(TABLE_REF, ROW, COLUMN, TIMESTAMP, LIMIT);
        executor.getNextColumnsForRow(TABLE_REF, ROW, COLUMN, LIMIT);
    }

}
