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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Counter;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.util.Pair;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

@SuppressWarnings("IllegalType") // explicitly need LinkedHashMap for insertion ordering contract
class RowColumnRangeExtractor {
    static class RowColumnRangeResult {
        private final Map<byte[], LinkedHashMap<Cell, Value>> results;
        private final Map<byte[], Column> rowsToLastCompositeColumns;
        private final Set<byte[]> emptyRows;
        private final Map<byte[], Integer> rowsToRawColumnCount;

        RowColumnRangeResult(
                Map<byte[], LinkedHashMap<Cell, Value>> results,
                Map<byte[], Column> rowsToLastCompositeColumns,
                Set<byte[]> emptyRows,
                Map<byte[], Integer> rowsToRawColumnCount) {
            this.results = results;
            this.rowsToLastCompositeColumns = rowsToLastCompositeColumns;
            this.emptyRows = emptyRows;
            this.rowsToRawColumnCount = rowsToRawColumnCount;
        }

        public Map<byte[], Map<Cell, Value>> getResults() {
            return Collections.unmodifiableMap(results);
        }

        public Map<byte[], Column> getRowsToLastCompositeColumns() {
            return Collections.unmodifiableMap(rowsToLastCompositeColumns);
        }

        public Set<byte[]> getEmptyRows() {
            return Collections.unmodifiableSet(emptyRows);
        }

        public Map<byte[], Integer> getRowsToRawColumnCount() {
            return Collections.unmodifiableMap(rowsToRawColumnCount);
        }
    }

    private final IdentityHashMap<byte[], LinkedHashMap<Cell, Value>> collector = new IdentityHashMap<>();
    private final IdentityHashMap<byte[], Column> rowsToLastCompositeColumns = new IdentityHashMap<>();
    private final IdentityHashMap<byte[], Integer> rowsToRawColumnCount = new IdentityHashMap<>();
    private final Set<byte[]> emptyRows = Collections.newSetFromMap(new IdentityHashMap<>()) ;
    private final Counter notLatestVisibleValueCellFilterCounter;

    RowColumnRangeExtractor(MetricsManager metricsManager) {
        notLatestVisibleValueCellFilterCounter = metricsManager.registerOrGetCounter(
                RowColumnRangeExtractor.class, AtlasDbMetricNames.CellFilterMetrics.NOT_LATEST_VISIBLE_VALUE);
    }

    public void extractResults(
            Iterable<byte[]> canonicalRows, Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey, long startTs) {
        // Make sure returned maps are keyed by the given rows
        Map<ByteBuffer, byte[]> canonicalRowsByHash = Maps.uniqueIndex(canonicalRows, ByteBuffer::wrap);
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> colEntry : colsByKey.entrySet()) {
            byte[] rawRow = CassandraKeyValueServices.getBytesFromByteBuffer(colEntry.getKey());
            byte[] row = canonicalRowsByHash.get(ByteBuffer.wrap(rawRow));
            List<ColumnOrSuperColumn> columns = colEntry.getValue();

            if (!columns.isEmpty()) {
                rowsToLastCompositeColumns.put(
                        row, columns.get(columns.size() - 1).getColumn());
            } else {
                emptyRows.add(row);
            }
            rowsToRawColumnCount.put(row, columns.size());
            for (ColumnOrSuperColumn c : columns) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(c.getColumn());
                internalExtractResult(startTs, row, pair.lhSide, c.getColumn().getValue(), pair.rhSide);
            }
        }
    }

    private void internalExtractResult(long startTs, byte[] row, byte[] col, byte[] val, long ts) {
        if (ts < startTs) {
            Cell cell = Cell.create(row, col);
            if (!collector.containsKey(row)) {
                collector.put(row, new LinkedHashMap<>());
                collector.get(row).put(cell, Value.create(val, ts));
            } else if (!collector.get(row).containsKey(cell)) {
                collector.get(row).put(cell, Value.create(val, ts));
            } else {
                notLatestVisibleValueCellFilterCounter.inc();
            }
        } else {
            notLatestVisibleValueCellFilterCounter.inc();
        }
    }

    public RowColumnRangeResult getRowColumnRangeResult() {
        return new RowColumnRangeResult(collector, rowsToLastCompositeColumns, emptyRows, rowsToRawColumnCount);
    }
}
