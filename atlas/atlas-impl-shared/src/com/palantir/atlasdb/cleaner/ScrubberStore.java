// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.cleaner;

import java.util.SortedMap;

import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Cell;

/**
 * This is the underlying store used by the scrubber for keeping track in a persistent way of the
 * cells that need to be scrubbed
 *
 * @author ejin
 */
public interface ScrubberStore {

    void queueCellsForScrubbing(Multimap<String, Cell> tableNameToCells, long scrubTimestamp, int batchSize);

    void markCellsAsScrubbed(Multimap<Cell, Long> cellToScrubTimestamp, int batchSize);

    SortedMap<Long, Multimap<String, Cell>> getCellsToScrub(int maxCellsToScrub, long maxScrubTimestamp /* exclusive */);

    int getNumberRemainingScrubCells(int maxCellsToScan);

}
