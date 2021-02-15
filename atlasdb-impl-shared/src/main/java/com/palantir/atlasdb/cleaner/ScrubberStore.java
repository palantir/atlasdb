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
package com.palantir.atlasdb.cleaner;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.processors.AutoDelegate;
import com.palantir.processors.DoDelegate;
import java.util.Map;
import java.util.SortedMap;

/**
 * This is the underlying store used by the scrubber for keeping track in a persistent way of the
 * cells that need to be scrubbed.
 *
 * @author ejin
 */
@AutoDelegate
public interface ScrubberStore {
    @DoDelegate
    default boolean isInitialized() {
        return true;
    }

    void queueCellsForScrubbing(Multimap<Cell, TableReference> cellToTableRefs, long scrubTimestamp, int batchSize);

    void markCellsAsScrubbed(Map<TableReference, Multimap<Cell, Long>> cellToScrubTimestamp, int batchSize);

    BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> getBatchingVisitableScrubQueue(
            long maxScrubTimestamp /* exclusive */, byte[] startRow, byte[] endRow);

    int getNumberRemainingScrubCells(int maxCellsToScan);
}
