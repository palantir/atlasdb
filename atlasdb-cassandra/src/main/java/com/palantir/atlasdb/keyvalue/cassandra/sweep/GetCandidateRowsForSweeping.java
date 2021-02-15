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
package com.palantir.atlasdb.keyvalue.cassandra.sweep;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweepingRequest;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;
import com.palantir.atlasdb.keyvalue.cassandra.paging.RowGetter;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GetCandidateRowsForSweeping {

    private static final int DEFAULT_TIMESTAMPS_BATCH_SIZE = 10_000;
    private static final int CONSERVATIVE_MAX_VALUES_BATCH_SIZE = 100;

    private final ValuesLoader valuesLoader;
    private final CqlExecutor cqlExecutor;
    private final RowGetter rowGetter;
    private final TableReference table;
    private final CandidateCellForSweepingRequest request;
    private final int timestampsBatchSize;
    private CassandraKeyValueServiceConfig config;
    private final int valuesBatchSize;

    private List<CellWithTimestamps> cellTimestamps;
    private Set<Cell> cellsWithEmptyValues;

    public GetCandidateRowsForSweeping(
            ValuesLoader valuesLoader,
            CqlExecutor cqlExecutor,
            RowGetter rowGetter,
            TableReference table,
            CandidateCellForSweepingRequest request,
            CassandraKeyValueServiceConfig config) {
        this.table = table;
        this.cqlExecutor = cqlExecutor;
        this.rowGetter = rowGetter;
        this.request = request;
        this.valuesLoader = valuesLoader;
        this.config = config;

        this.timestampsBatchSize = request.batchSizeHint().orElse(DEFAULT_TIMESTAMPS_BATCH_SIZE);
        // TODO(nziebart): this should probably be configurable
        this.valuesBatchSize = Math.max(1, Math.min(CONSERVATIVE_MAX_VALUES_BATCH_SIZE, timestampsBatchSize / 32));
    }

    /**
     * Fetches a batch of candidate rows. The returned {@link CandidateRowForSweeping}s are ordered by row.
     */
    public List<CandidateRowForSweeping> execute() {
        fetchCellTimestamps();

        findCellsWithEmptyValuesIfNeeded();

        return convertToOrderedSweepCandidateRows();
    }

    private void fetchCellTimestamps() {
        cellTimestamps = new GetCellTimestamps(
                        cqlExecutor, rowGetter, table, request.startRowInclusive(), timestampsBatchSize, config)
                .execute();
    }

    public void findCellsWithEmptyValuesIfNeeded() {
        if (!request.shouldCheckIfLatestValueIsEmpty()) {
            cellsWithEmptyValues = Collections.emptySet();
            return;
        }

        cellsWithEmptyValues = new GetEmptyLatestValues(
                        cellTimestamps, valuesLoader, table, request.maxTimestampExclusive(), valuesBatchSize)
                .execute();
    }

    private List<CandidateRowForSweeping> convertToOrderedSweepCandidateRows() {
        Map<ByteBuffer, List<CandidateCellForSweeping>> cellsByRow = cellTimestamps.stream()
                .map(cell -> cell.toSweepCandidate(request::shouldSweep, cellsWithEmptyValues.contains(cell.cell())))
                .collect(Collectors.groupingBy(
                        cell -> ByteBuffer.wrap(cell.cell().getRowName()), LinkedHashMap::new, Collectors.toList()));

        List<CandidateRowForSweeping> candidates = new ArrayList<>();
        cellsByRow.forEach((row, cells) -> {
            Preconditions.checkState(row.hasArray(), "Expected an array backed buffer");
            Preconditions.checkState(row.arrayOffset() == 0, "Buffer array must have no offset");
            Preconditions.checkState(row.limit() == row.array().length, "Array length must match the limit");
            candidates.add(CandidateRowForSweeping.of(
                    row.array(),
                    cells.stream()
                            .sorted(Comparator.comparing(CandidateCellForSweeping::cell))
                            .collect(Collectors.toList())));
        });
        return candidates;
    }
}
