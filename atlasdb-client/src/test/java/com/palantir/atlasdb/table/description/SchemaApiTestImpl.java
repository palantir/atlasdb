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

package com.palantir.atlasdb.table.description;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.table.description.generated.ApiTestTableFactory;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable.SchemaApiTestRow;
import com.palantir.atlasdb.table.description.generated.SchemaApiTestTable.SchemaApiTestRowResult;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;

public class SchemaApiTestImpl extends AbstractSchemaApiTest {

    private static final ApiTestTableFactory tableFactory = ApiTestTableFactory.of();

    @Override
    protected void putSingleRowFirstColumn(Transaction transaction, String rowKey, long value) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);
        table.putColumn1(SchemaApiTestRow.of(rowKey), value);
    }

    @Override
    protected Long getSingleRowFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);

        ColumnSelection firstColSelection = SchemaApiTestTable.getColumnSelection(
                SchemaApiTestTable.SchemaApiTestNamedColumn.COLUMN1);
        Optional<SchemaApiTestRowResult> result = table.getRow(SchemaApiTestRow.of(rowKey), firstColSelection);
        return result.get().getColumn1();
    }

    @Override
    protected Map<String, Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKeys) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);

        ColumnSelection firstColSelection = SchemaApiTestTable.getColumnSelection(
                SchemaApiTestTable.SchemaApiTestNamedColumn.COLUMN1);
        List<SchemaApiTestRowResult> result =
                table.getRows(
                        rowKeys.stream().map(SchemaApiTestRow::of).collect(Collectors.toList()),
                        firstColSelection);
        return result
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getRowName().getComponent1(),
                        SchemaApiTestTable.SchemaApiTestRowResult::getColumn1));
    }

    @Override
    protected Map<String, String> getRangeSecondColumn(Transaction transaction, String startRowKey, String endRowKey) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);

        ColumnSelection secondColSelection = SchemaApiTestTable.getColumnSelection(
                SchemaApiTestTable.SchemaApiTestNamedColumn.COLUMN2);

        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(SchemaApiTestRow.of(startRowKey).persistToBytes())
                .endRowExclusive(SchemaApiTestRow.of(endRowKey).persistToBytes())
                .retainColumns(secondColSelection)
                .build();

        BatchingVisitableView<SchemaApiTestRowResult> rangeRequestResult = table.getRange(rangeRequest);
        return rangeRequestResult.immutableCopy()
                .stream()
                .collect(Collectors.toMap(
                        entry -> entry.getRowName().getComponent1(),
                        SchemaApiTestTable.SchemaApiTestRowResult::getColumn2));
    }

    @Override
    protected void deleteWholeRow(Transaction transaction, String rowKey) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);
        table.delete(SchemaApiTestRow.of(rowKey));
    }

    @Override
    protected void deleteFirstColumn(Transaction transaction, String rowKey) {
        SchemaApiTestTable table = tableFactory.getSchemaApiTestTable(transaction);
        table.deleteColumn1(SchemaApiTestRow.of(rowKey));
    }
}
