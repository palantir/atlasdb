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

package com.palantir.atlasdb.cell.api;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import java.util.Map;

public interface DataTableCellDeleter {
    /**
     * Adds a value with timestamp = Value.INVALID_VALUE_TIMESTAMP to each of the given cells. If
     * a value already exists at that time stamp, nothing is written for that cell.
     */
    void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells);

    /**
     * For each cell, deletes all timestamps prior to the associated maximum timestamp. If this
     * operation fails, it's acceptable for this method to leave an inconsistent state, however
     * implementations of this method <b>must</b> guarantee that, for each cell, if a value at the
     * associated timestamp is inconsistently deleted, then all other values of that cell in the
     * relevant range must have already been consistently deleted.
     *
     * @param tableRef the name of the table to delete the timestamps in.
     * @param deletes cells to be deleted, and the ranges of timestamps to delete for each cell
     */
    void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes)
            throws InsufficientConsistencyException;

    /**
     * Returns true iff the user table exists. This is useful for knowing when entries that correspond to tables
     * that no longer exist can be safely ignored.
     */
    boolean doesUserDataTableExist(TableReference tableRef);

    /**
     * Returns whether this transaction key value service is guaranteed to be a source of truth for the broader system
     * at the given timestamp.
     */
    boolean isValid(long timestamp);
}
