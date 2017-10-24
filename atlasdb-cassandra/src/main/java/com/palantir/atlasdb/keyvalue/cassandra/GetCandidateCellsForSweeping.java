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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.GetEmptyLatestValues;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.paging.CellWithTimestamps;

public class GetCandidateCellsForSweeping {

    private final KeyValueService kvs;
    private final CqlExecutor cqlExecutor;
    private final TableReference table;
    private final CandidateCellForSweepingRequest request;

    private List<CellWithTimestamps> cellTimestamps;
    private Set<Cell> cellsWithEmptyValues;

    public GetCandidateCellsForSweeping(
            KeyValueService kvs,
            CqlExecutor cqlExecutor,
            TableReference table,
            CandidateCellForSweepingRequest request) {
        this.table = table;
        this.cqlExecutor = cqlExecutor;
        this.request = request;
        this.kvs = kvs;
    }

    public List<CandidateCellForSweeping> execute() {
        fetchCellTimestamps();

        findCellsWithEmptyValuesIfNeeded();

        return convertToSweepCandidates();
    }

    private void fetchCellTimestamps() {
        cellTimestamps = new GetCellTimestamps(
                cqlExecutor,
                table,
                request.startRowInclusive(),
                request.batchSizeHint().orElse(10_000))
                .execute();
    }

    public void findCellsWithEmptyValuesIfNeeded() {
        if (!request.shouldCheckIfLatestValueIsEmpty()) {
            cellsWithEmptyValues = Collections.emptySet();
            return;
        }

        cellsWithEmptyValues = new GetEmptyLatestValues(cellTimestamps, kvs, table, request.maxTimestampExclusive(),
                100).execute();
    }

    private List<CandidateCellForSweeping> convertToSweepCandidates() {
        return Lists.transform(
                cellTimestamps,
                cell -> cell.toSweepCandidate(
                        request.maxTimestampExclusive(),
                        cellsWithEmptyValues.contains(cell.cell())));
    }
}
