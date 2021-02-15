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
package com.palantir.atlasdb.table.description;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.table.description.generated.ApiTestTableFactory;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestV2Table;
import com.palantir.atlasdb.table.description.test.StringValue;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractTransaction;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class SchemaApiTestV2Impl extends AbstractSchemaApiTest {

    private static final ApiTestTableFactory tableFactory = ApiTestTableFactory.of();

    @Override
    protected void putSingleRowFirstColumn(Transaction transaction, String rowKey, long value) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.putColumn1(rowKey, value);
    }

    @Override
    protected Long getSingleRowFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        Optional<Long> result = table.getColumn1(rowKey);
        return result.get();
    }

    @Override
    protected Map<String, Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKeys) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        return table.getColumn1(rowKeys);
    }

    @Override
    protected Map<String, StringValue> getRangeSecondColumn(
            Transaction transaction, String startRowKey, String endRowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        return table.getSmallRowRangeColumn2(startRowKey, endRowKey);
    }

    @Override
    protected Map<String, StringValue> getRangeSecondColumnOnlyFirstTwoResults(
            Transaction transaction, String startRowKey, String endRowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);

        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(
                        SchemaApiTestTable.SchemaApiTestRow.of(startRowKey).persistToBytes())
                .endRowExclusive(
                        SchemaApiTestTable.SchemaApiTestRow.of(endRowKey).persistToBytes())
                .build();

        return table.getSmallRowRangeColumn2(rangeRequest, 2);
    }

    @Override
    protected void deleteWholeRow(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.deleteRow(rowKey);
    }

    @Override
    protected void deleteFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestV2Table table = tableFactory.getSchemaApiTestV2Table(transaction);
        table.deleteColumn1(rowKey);
    }

    @Test
    public void testUpdateEntryIfEntryExists() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        SchemaApiTestV2Table table = spy(tableFactory.getSchemaApiTestV2Table(transaction));
        doReturn(Optional.of(TEST_VALUE_LONG)).when(table).getColumn1(TEST_ROW_KEY);

        table.updateColumn1(TEST_ROW_KEY, entry -> entry + 1);

        verify(table, times(1)).putColumn1(TEST_ROW_KEY, TEST_VALUE_LONG + 1);
    }

    @Test
    public void testUpdateEntryIfEntryDoesNotExist() {
        AbstractTransaction transaction = mock(AbstractTransaction.class);
        SchemaApiTestV2Table table = spy(tableFactory.getSchemaApiTestV2Table(transaction));
        doReturn(Optional.empty()).when(table).getColumn1(TEST_ROW_KEY);

        table.updateColumn1(TEST_ROW_KEY, entry -> entry + 1);

        verify(table, never()).putColumn1(any(), anyLong());
    }
}
