/**
 * Copyright 2017 Palantir Technologies
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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;

public class CassandraTimestampStoreInvalidatorTest {
    private static final long CASSANDRA_TIMESTAMP = 0L;
    private static final long FORTY_TWO = 42L;
    private static final byte[] FORTY_TWO_IN_BYTES = PtBytes.toBytes(FORTY_TWO);
    private static final long FORTY_THREE = 43L;
    private static final byte[] FORTY_THREE_IN_BYTES = PtBytes.toBytes(FORTY_THREE);

    private final CassandraKeyValueService kvs = mock(CassandraKeyValueService.class);
    private final CassandraTimestampCqlExecutor cqlExecutor = mock(CassandraTimestampCqlExecutor.class);
    private final CassandraTimestampStoreInvalidator invalidator =
            new CassandraTimestampStoreInvalidator(kvs, cqlExecutor);

    @Before
    public void setUp() {
        when(kvs.get(any(), any())).thenAnswer(this::constructKeyValueStoreReply);
    }

    private Map<Cell, Value> constructKeyValueStoreReply(InvocationOnMock invocation) {
        Map<Cell, Long> queries = (Map<Cell, Long>) invocation.getArguments()[1];
        Cell cell = queries.keySet().iterator().next();

        Value valueToReturn = Value.create(FORTY_THREE_IN_BYTES, CASSANDRA_TIMESTAMP);
        if (Arrays.equals(cell.getRowName(), CassandraTimestampCqlExecutor.ROW_AND_COLUMN_NAME_BYTES)) {
            valueToReturn = Value.create(FORTY_TWO_IN_BYTES, CASSANDRA_TIMESTAMP);
        }
        return ImmutableMap.of(cell, valueToReturn);
    }

    @Test
    public void invalidatePassesCurrentTimestampValueFromKvsToBeBackedUp() {
        invalidator.invalidateTimestampStore();
        verify(cqlExecutor, times(1)).backupBound(eq(FORTY_TWO));
    }

    @Test
    public void revalidatePassesBackupValueFromKvsToBeBackedUp() {
        invalidator.revalidateTimestampStore();
        verify(cqlExecutor, times(1)).restoreBoundFromBackup(eq(FORTY_THREE));
    }
}
