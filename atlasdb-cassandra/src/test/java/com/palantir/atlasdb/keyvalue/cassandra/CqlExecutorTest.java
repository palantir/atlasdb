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
    public void getTimestamps() {
        String expected = "SELECT key, column1, column2 FROM \"foo__bar\" WHERE token(key) >= token(0x0102) LIMIT 100;";

        executor.getTimestamps(TABLE_REF, ROW, LIMIT);

        verify(queryExecutor).execute(ROW, expected);
    }

    @Test
    public void getTimestampsWithinRow() {
        String expected = "SELECT column1, column2 FROM \"foo__bar\" WHERE key = 0x0102"
                + " AND (column1, column2) > (0x0304, -124) LIMIT 100;";

        executor.getTimestampsWithinRow(TABLE_REF, ROW, COLUMN, TIMESTAMP, LIMIT);

        verify(queryExecutor).execute(ROW, expected);
    }

}
