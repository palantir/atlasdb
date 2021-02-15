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
package com.palantir.atlasdb.transaction.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public interface ConstraintCheckingTransaction extends Transaction {
    /**
     * Returns the cell values in the key-value service, ignoring the cache of local writes.
     */
    Map<Cell, byte[]> getIgnoringLocalWrites(TableReference tableRef, Set<Cell> cells);

    /**
     * Returns the row result values in the key-value service, ignoring the cache of local writes.
     */
    SortedMap<byte[], RowResult<byte[]>> getRowsIgnoringLocalWrites(TableReference tableRef, Iterable<byte[]> rows);
}
