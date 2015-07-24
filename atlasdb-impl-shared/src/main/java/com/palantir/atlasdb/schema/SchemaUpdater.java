// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.schema;

import java.util.Map;

import com.google.common.base.Function;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.LockAwareTransactionManager;

public interface SchemaUpdater extends SimpleSchemaUpdater {

    /**
     * Changes the row name of the source table by copying it to a new destination table with a row
     * name transform. This method does not create or drop tables.
     * <p>
     * See {@link RowTransformers} for common ways of constructing a row name transform.
     *
     * @param srcTable The table containing the row to change.
     * @param destTable The new table to copy into. This table must already exist.
     * @param rowNameTransform The transformation to perform from the current row name to the new row name.
     */
    void changeRowName(String srcTable,
                       String destTable,
                       Function<byte[], byte[]> rowNameTransform);

    /**
     * Copy all rows of srcTable to destTable, transforming those rows along the way.
     * <p>
     * Use {@link EncodingUtils} to transform rows and columns into their constituent components.
     *
     * @param srcTable The table to copy.
     * @param destTable The table to copy into. This table must already exist.
     * @param rowTransformer The transformation to perform from an existing row to new rows.
     * @param columnSelection The columns to select from srcTable.
     */
    void copyTable(String srcTable,
                   String destTable,
                   Function<RowResult<byte[]>, Map<Cell, byte[]>> rowTransformer,
                   ColumnSelection columnSelection);

    LockAwareTransactionManager getTxManager();

    /**
     * Create a new StreamStore with the given long name, short name, and value type.
     */
    void addStreamStore(String longName, String shortName, ValueType streamIdType);
}
