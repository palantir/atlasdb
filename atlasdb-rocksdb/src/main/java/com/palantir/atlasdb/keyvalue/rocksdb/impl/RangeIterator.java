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
package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import java.util.Arrays;
import java.util.SortedMap;

import org.rocksdb.RocksIterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ColumnFamilyMap.ColumnFamily;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.Pair;


abstract class RangeIterator<T> extends AbstractIterator<RowResult<T>> implements ClosableIterator<RowResult<T>> {
    private final ColumnFamily table;
    protected final RocksIterator it;
    private final RangeRequest request;
    protected final long maxTimestamp;

    RangeIterator(ColumnFamily table, RocksIterator it, RangeRequest range, long maxTimestamp) {
        this.table = table;
        this.it = it;
        this.request = range;
        this.maxTimestamp = maxTimestamp;
        byte[] start = range.getStartInclusive();
        if (start.length == 0) {
            it.seekToFirst();
        } else {
            it.seek(RocksDbKeyValueServices.getKey(start, maxTimestamp - 1));
        }
    }

    @Override
    protected RowResult<T> computeNext() {
        while (it.isValid()) {
            Pair<Cell, Long> cellAndTs = RocksDbKeyValueServices.parseCellAndTs(it.key());
            Cell cell = cellAndTs.lhSide;
            if (!RocksDbKeyValueServices.isInRange(cell.getRowName(), request.getEndExclusive())) {
                break;
            }
            byte[] row = cell.getRowName();
            ImmutableSortedMap.Builder<byte[], T> builder = ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
            do {
                T value = processCell(cellAndTs);
                if (value != null && request.containsColumn(cell.getColumnName())) {
                    builder.put(cell.getColumnName(), value);
                }
                if (!it.isValid()) {
                    break;
                }
                cellAndTs = RocksDbKeyValueServices.parseCellAndTs(it.key());
                cell = cellAndTs.lhSide;
            } while (Arrays.equals(row, cell.getRowName()));
            SortedMap<byte[], T> columns = builder.build();
            if (!columns.isEmpty()) {
                return RowResult.create(row, columns);
            }
        }
        return endOfData();
    }

    protected abstract T processCell(Pair<Cell, Long> cellAndInitialTs);

    @Override
    public void close() {
        it.dispose();
        table.close();
    }
}
