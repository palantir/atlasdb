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

package com.palantir.atlasdb.schema;

import java.util.Collections;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.table.description.generated.GenericTestTable;

public class SchemaV1Test extends AbstractSchemaTest{

    @Override
    protected void putSingleRowFirstColumn(GenericTestTable table, String rowKey, long value) {
        table.putColumn1(GenericTestTable.GenericTestRow.of(rowKey), value);
    }

    @Override
    protected void getSingleRowAllColumns(GenericTestTable table, String rowKey) {
        table.getRows(Collections.singletonList(GenericTestTable.GenericTestRow.of(rowKey)));
    }

    @Override
    protected void getSingleRowFirstColumn(GenericTestTable table, String rowKey) {
        ColumnSelection firstColSelection =
                ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        table.getRows(Collections.singletonList(GenericTestTable.GenericTestRow.of(rowKey)), firstColSelection);
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
