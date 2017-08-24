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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.table.description.generated.GenericTestTable;

public class SchemaApiTest extends AbstractSchemaTest {

    @Override
    protected void putSingleRowFirstColumn(GenericTestTable table, String rowKey, long value) {
        table.putColumn1(GenericTestTable.GenericTestRow.of(rowKey), value);
    }

    @Override
    protected Long getSingleRowFirstColumn(GenericTestTable table, String rowKey) {
        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        Optional<GenericTestTable.GenericTestRowResult> result =
                table.getRow(GenericTestTable.GenericTestRow.of(rowKey), firstColSelection);
        return result.get().getColumn1();
    }

    @Override
    protected List<Long> getMultipleRowsFirstColumn(GenericTestTable table, List<String> rowKeys) {
        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        List<GenericTestTable.GenericTestRowResult> result =
                table.getRows(
                        rowKeys.stream().map(GenericTestTable.GenericTestRow::of).collect(Collectors.toList()),
                        firstColSelection
                );
        return result.stream().map(GenericTestTable.GenericTestRowResult::getColumn1).collect(Collectors.toList());
    }

    @Override
    protected void deleteWholeRow(GenericTestTable table, String rowKey) {
        table.delete(GenericTestTable.GenericTestRow.of(rowKey));
    }

    @Override
    protected void deleteFirstColumn(GenericTestTable table, String rowKey) {
        table.deleteColumn1(GenericTestTable.GenericTestRow.of(rowKey));
    }
}
