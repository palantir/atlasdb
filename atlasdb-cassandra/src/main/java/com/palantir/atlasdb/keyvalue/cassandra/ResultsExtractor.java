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
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.RowResults;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.util.Pair;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;

public abstract class ResultsExtractor<T> {

    protected final MetricsManager metricsManager;

    public ResultsExtractor(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    @SuppressWarnings("VisibilityModifier")
    public final byte[] extractResults(
            Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey, long startTs, ColumnSelection selection) {
        byte[] maxRow = null;
        for (Map.Entry<ByteBuffer, List<ColumnOrSuperColumn>> colEntry : colsByKey.entrySet()) {
            byte[] row = CassandraKeyValueServices.getBytesFromByteBuffer(colEntry.getKey());
            maxRow = updatedMaxRow(maxRow, row);

            for (ColumnOrSuperColumn c : colEntry.getValue()) {
                Pair<byte[], Long> pair = CassandraKeyValueServices.decomposeName(c.getColumn());
                internalExtractResult(
                        startTs, selection, row, pair.lhSide, c.getColumn().getValue(), pair.rhSide);
            }
        }
        return maxRow;
    }

    private byte[] updatedMaxRow(byte[] previousMaxRow, byte[] row) {
        if (previousMaxRow == null) {
            return row;
        } else {
            return PtBytes.BYTES_COMPARATOR.max(previousMaxRow, row);
        }
    }

    public TokenBackedBasicResultsPage<RowResult<T>, byte[]> getPageFromRangeResults(
            Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey,
            long startTs,
            ColumnSelection selection,
            byte[] endExclusive) {
        byte[] lastRow = extractResults(colsByKey, startTs, selection);
        NavigableMap<byte[], NavigableMap<byte[], T>> resultsByRow = Cells.breakCellsUpByRow(asMap());
        return getRowResults(endExclusive, lastRow, resultsByRow);
    }

    public static <T> TokenBackedBasicResultsPage<RowResult<T>, byte[]> getRowResults(
            byte[] endExclusive, byte[] lastRow, NavigableMap<byte[], NavigableMap<byte[], T>> resultsByRow) {
        NavigableMap<byte[], RowResult<T>> ret = RowResults.viewOfSortedMap(resultsByRow);
        if (lastRow == null || RangeRequests.isLastRowName(lastRow)) {
            return new SimpleTokenBackedResultsPage<>(endExclusive, ret.values(), false);
        }
        byte[] nextStart = RangeRequests.nextLexicographicName(lastRow);
        if (Arrays.equals(nextStart, endExclusive)) {
            return new SimpleTokenBackedResultsPage<>(endExclusive, ret.values(), false);
        }
        return new SimpleTokenBackedResultsPage<>(nextStart, ret.values(), true);
    }

    public abstract void internalExtractResult(
            long startTs, ColumnSelection selection, byte[] row, byte[] col, byte[] val, long ts);

    public abstract Map<Cell, T> asMap();

    protected Counter getNotLatestVisibleValueCellFilterCounter(Class clazz) {
        // TODO(hsaraogi): add table names as a tag
        return metricsManager.registerOrGetCounter(
                clazz, AtlasDbMetricNames.CellFilterMetrics.NOT_LATEST_VISIBLE_VALUE);
    }
}
