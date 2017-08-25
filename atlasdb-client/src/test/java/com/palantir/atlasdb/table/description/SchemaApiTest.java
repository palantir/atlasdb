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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.GenericTestTable;
import com.palantir.atlasdb.table.description.generated.GenericTestTable.GenericTestRow;
import com.palantir.atlasdb.table.description.generated.GenericTestTable.GenericTestRowResult;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;

public class SchemaApiTest extends AbstractSchemaTest {

    private static final GenericTestSchemaTableFactory tableFactory = GenericTestSchemaTableFactory.of();

    @Override
    protected void putSingleRowFirstColumn(Transaction transaction, String rowKey, long value) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        table.putColumn1(GenericTestRow.of(rowKey), value);
    }

    @Override
    protected Long getSingleRowFirstColumn(Transaction transaction, String rowKey) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);

        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes(firstColShortName)));
        Optional<GenericTestRowResult> result = table.getRow(GenericTestRow.of(rowKey), firstColSelection);
        return result.get().getColumn1();
    }

    @Override
    protected List<Long> getMultipleRowsFirstColumn(Transaction transaction, List<String> rowKeys) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);

        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes(firstColShortName)));
        List<GenericTestRowResult> result =
                table.getRows(
                        rowKeys.stream().map(GenericTestRow::of).collect(Collectors.toList()),
                        firstColSelection
                );
        return result.stream().map(GenericTestRowResult::getColumn1).collect(Collectors.toList());
    }

    @Override
    protected List<String> getRangeSecondColumn(Transaction transaction, String startRowKey, String endRowKey) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);

        ColumnSelection secondColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes(secondColShortName)));

        RangeRequest rangeRequest = RangeRequest.builder()
                .startRowInclusive(GenericTestRow.of(startRowKey).persistToBytes())
                .endRowExclusive(GenericTestRow.of(endRowKey).persistToBytes())
                .retainColumns(secondColSelection)
                .build();

        BatchingVisitableView<GenericTestRowResult> rangeRequestResult = table.getRange(rangeRequest);
        ArrayList finalResult = new ArrayList<>();
        rangeRequestResult.forEach(entry -> finalResult.add(entry.getColumn2()));
        return finalResult;
    }

    @Override
    protected void deleteWholeRow(Transaction transaction, String rowKey) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        table.delete(GenericTestRow.of(rowKey));
    }

    @Override
    protected void deleteFirstColumn(Transaction transaction, String rowKey) {
        GenericTestTable table = tableFactory.getGenericTestTable(transaction);
        table.deleteColumn1(GenericTestRow.of(rowKey));
    }
}
