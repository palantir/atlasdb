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

package com.palantir.atlasdb.keyvalue.api;

import com.google.common.collect.Multimap;
import com.palantir.common.base.ClosableIterator;
import java.util.List;
import java.util.Map;

public interface SweepKeyValueService {
    byte[] getMetadataForTable(TableReference tableRef);

    Map<TableReference, byte[]> getMetadataForTables();

    boolean sweepsEntriesInStrictlyNonDecreasingFashion();

    void addGarbageCollectionSentinelValues(TableReference tableRef, Iterable<Cell> cells);

    void delete(TableReference tableRef, Multimap<Cell, Long> keys);

    ClosableIterator<List<CandidateCellForSweeping>> getCandidateCellsForSweeping(
            TableReference tableRef, CandidateCellForSweepingRequest request);
}
