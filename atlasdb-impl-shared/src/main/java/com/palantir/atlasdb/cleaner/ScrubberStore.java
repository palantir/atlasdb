/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import java.util.Map;
import java.util.SortedMap;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.BatchingVisitable;

/**
 * This is the underlying store used by the scrubber for keeping track in a persistent way of the
 * cells that need to be scrubbed.
 *
 * @author ejin
 */
public interface ScrubberStore {
    void queueCellsForScrubbing(Multimap<Cell, TableReference> cellToTableRefs, long scrubTimestamp, int batchSize);

    void markCellsAsScrubbed(Map<TableReference, Multimap<Cell, Long>> cellToScrubTimestamp, int batchSize);

    BatchingVisitable<SortedMap<Long, Multimap<TableReference, Cell>>> getBatchingVisitableScrubQueue(
            long maxScrubTimestamp /* exclusive */,
            byte[] startRow,
            byte[] endRow);

    int getNumberRemainingScrubCells(int maxCellsToScan);
}
