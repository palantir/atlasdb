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

package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.StreamStoreRemappingNextTableToSweepProviderImpl;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamStoreRemappingNextTableToSweepProviderTest {
    private static final String TEST_TABLE = "test";
    private static final String SS_VALUE_TABLE_NAME = StreamTableType.VALUE.getTableName(TEST_TABLE);
    private static final String SS_INDEX_TABLE_NAME = StreamTableType.INDEX.getTableName(TEST_TABLE);

    private static final TableReference NOT_SS_VALUE_TABLE = TableReference.createWithEmptyNamespace("test");
    private static final TableReference SS_VALUE_TABLE = TableReference.createWithEmptyNamespace(SS_VALUE_TABLE_NAME);
    private static final TableReference SS_INDEX_TABLE = TableReference.createWithEmptyNamespace(SS_INDEX_TABLE_NAME);

    private NextTableToSweepProviderImpl delegate = mock(NextTableToSweepProviderImpl.class);
    private StreamStoreRemappingNextTableToSweepProviderImpl provider =
            new StreamStoreRemappingNextTableToSweepProviderImpl(delegate);

    private Transaction mockedTransaction = mock(Transaction.class);

    @Test
    public void notValueTableReturnsSameTable() {
        Optional<TableReference> selectedTable = Optional.of(NOT_SS_VALUE_TABLE);
        when(delegate.chooseNextTableToSweep(any(), anyLong())).thenReturn(selectedTable);

        Optional<TableReference> returnedTable = provider.chooseNextTableToSweep(mockedTransaction, 1L);
        assertThat(returnedTable).isEqualTo(selectedTable);
    }

    @Test
    public void valueTableReturnsIndexThenValueTables() {
        Optional<TableReference> selectedTable = Optional.of(SS_VALUE_TABLE);
        when(delegate.chooseNextTableToSweep(any(), anyLong())).thenReturn(selectedTable);

        assertReturnsIndexThenValueTable();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void notValueTableAfterValueTableIsReturnedCorrectly() {
        Optional<TableReference> selectedTable = Optional.of(SS_VALUE_TABLE);
        Optional<TableReference> nextSelectedTable = Optional.of(NOT_SS_VALUE_TABLE);
        when(delegate.chooseNextTableToSweep(any(), anyLong())).thenReturn(selectedTable, nextSelectedTable);

        assertReturnsIndexThenValueTable();

        Optional<TableReference> followupReturnedTable = provider.chooseNextTableToSweep(mockedTransaction, 1L);
        assertThat(followupReturnedTable).isEqualTo(Optional.of(NOT_SS_VALUE_TABLE));
    }

    private void assertReturnsIndexThenValueTable() {
        Optional<TableReference> returnedTable = provider.chooseNextTableToSweep(mockedTransaction, 1L);
        assertThat(returnedTable).isEqualTo(Optional.of(SS_INDEX_TABLE));

        Optional<TableReference> followupReturnedTable = provider.chooseNextTableToSweep(mockedTransaction, 1L);
        assertThat(followupReturnedTable).isEqualTo(Optional.of(SS_VALUE_TABLE));
    }
}
