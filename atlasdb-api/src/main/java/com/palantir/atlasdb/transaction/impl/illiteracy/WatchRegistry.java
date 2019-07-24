/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Map;
import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.CellReference;

public interface WatchRegistry {
    // Precondition: Table must have row level conflict handling
    void enableWatchForRows(Set<RowReference> rowReference);

    // Returns true iff the watch was actually removed
    Set<RowReference> disableWatchForRows(Set<RowReference> rowReference);

    // Returns the most precise available reference one has into the row cache.
    // Alternatively, a minimum set of references - if any of these are unchanged then the cache is treated as valid.
    Map<RowReference, RowCacheReference> filterToWatchedRows(Set<RowReference> rowReferenceSet);

    // Precondition: Table must have row level conflict handling
    void enableWatchForRowPrefix(RowPrefixReference prefixReference);

    // Precondition: Table must have cell level conflict handling
    void enableWatchForCells(Set<CellReference> cellReference);

    // Returns true iff the watch was actually removed
    Set<CellReference> disableWatchForCells(Set<CellReference> cellReference);

    // Returns the cells that are watched
    Set<CellReference> filterToWatchedCells(Set<CellReference> rowReferenceSet);
}
