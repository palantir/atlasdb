/**
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

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.util.Pair;

class RowColumnRangeExtractor {
    static class RowColumnRangeResult {
        final Map<byte[], LinkedHashMap<Cell, Value>> results;
        final Map<byte[], Column> rowsToLastCompositeColumns;

        public RowColumnRangeResult(Map<byte[], LinkedHashMap<Cell, Value>> results, Map<byte[], Column> rowsToLastCompositeColumns) {
            this.results = results;
            this.rowsToLastCompositeColumns = rowsToLastCompositeColumns;
        }
    }

    private final Map<byte[], LinkedHashMap<Cell, Value>> collector = Maps.newHashMap();
    private final Map<byte[], Column> rowsToLastCompositeColumns = Maps.newHashMap();

    public void extractResults(Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey,
                                                long startTs,
                                                ColumnSelection selection) {
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> colEntry : colsByKey.entrySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(colEntry.getKey());
            List<ColumnOrSuperColumn> columns = colEntry.getValue();
            if (!columns.isEmpty()) {
                rowsToLastCompositeColumns.put(row, columns.get(columns.size() - 1).column);
            }
            for (ColumnOrSuperColumn c : colEntry.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(c.column);
                internalExtractResult(startTs, selection, row, pair.lhSide, c.column.getValue(), pair.rhSide);
            }
        }
    }

    public void internalExtractResult(long startTs,
                                      ColumnSelection selection,
                                      byte[] row,
                                      byte[] col,
                                      byte[] val,
                                      long ts) {
        if (ts < startTs && selection.contains(col)) {
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
        return new RowColumnRangeResult(collector, rowsToLastCompositeColumns);
    }
}
