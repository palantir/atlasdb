/*
 * Copyright 2015 Palantir Technologies
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

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.Pair;

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

        public Map<byte[], LinkedHashMap<Cell, Value>> getResults() {
            return results;
        }

        public Map<byte[], Column> getRowsToLastCompositeColumns() {
            return rowsToLastCompositeColumns;
        }

        public Set<byte[]> getEmptyRows() {
            return emptyRows;
        }

        public Map<byte[], Integer> getRowsToRawColumnCount() {
            return rowsToRawColumnCount;
        }
    }

    private final Map<byte[], LinkedHashMap<Cell, Value>> collector = Maps.newHashMap();
    private final Map<byte[], Column> rowsToLastCompositeColumns = Maps.newHashMap();
    private final Map<byte[], Integer> rowsToRawColumnCount = Maps.newHashMap();
    private final Set<byte[]> emptyRows = Sets.newHashSet();

    public void extractResults(Iterable<byte[]> canonicalRows,
                               Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey,
                               long startTs) {
        // Make sure returned maps are keyed by the given rows
        Map<ByteBuffer, byte[]> canonicalRowsByHash = Maps.uniqueIndex(canonicalRows, ByteBuffer::wrap);
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> colEntry : colsByKey.entrySet()) {
            byte[] rawRow = CassandraKeyValueServices.getBytesFromByteBuffer(colEntry.getKey());
            byte[] row = canonicalRowsByHash.get(ByteBuffer.wrap(rawRow));
            List<ColumnOrSuperColumn> columns = colEntry.getValue();
            if (!columns.isEmpty()) {
                rowsToLastCompositeColumns.put(row, columns.get(columns.size() - 1).column);
            } else {
                emptyRows.add(row);
            }
            rowsToRawColumnCount.put(row, columns.size());
            for (ColumnOrSuperColumn c : colEntry.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(c.column);
                internalExtractResult(startTs, row, pair.lhSide, c.column.getValue(), pair.rhSide);
            }
        }
    }

    private void internalExtractResult(long startTs, byte[] row, byte[] col, byte[] val, long ts) {
        if (ts < startTs) {
            Cell cell = Cell.create(row, col);
            if (!collector.containsKey(row)) {
                collector.put(row, new LinkedHashMap<Cell, Value>());
                collector.get(row).put(cell, Value.create(val, ts));
            } else if (!collector.get(row).containsKey(cell)) {
                collector.get(row).put(cell, Value.create(val, ts));
            }
        }
    }

    public RowColumnRangeResult getRowColumnRangeResult() {
        return new RowColumnRangeResult(collector, rowsToLastCompositeColumns, emptyRows, rowsToRawColumnCount);
    }
}
