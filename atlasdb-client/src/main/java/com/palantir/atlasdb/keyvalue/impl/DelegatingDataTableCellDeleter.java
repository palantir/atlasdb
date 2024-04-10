/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import com.palantir.atlasdb.cell.api.DataTableCellDeleter;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import java.util.Map;

public class DelegatingDataTableCellDeleter implements DataTableCellDeleter {
    private final KeyValueService delegate;

    public DelegatingDataTableCellDeleter(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells) {
        delegate.addGarbageCollectionSentinelValues(tableRef, cells);
    }

    @Override
    public void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes)
            throws InsufficientConsistencyException {
        delegate.deleteAllTimestamps(tableRef, deletes);
    }

    @Override
    public boolean doesUserDataTableExist(TableReference tableRef) {
        return delegate.getAllTableNames().contains(tableRef);
    }

    @Override
    public boolean isValid(long timestamp) {
        // May be restricted above this layer, but at this level always true.
        return true;
    }
}
