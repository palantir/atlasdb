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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.rocksdb.RocksIterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.common.annotation.Output;
import com.palantir.util.Pair;

public class RocksDbKeyValueServices {

    private RocksDbKeyValueServices() {
        // cannot instantiate
    }


    static boolean keyExists(RocksIterator iter, byte[] key) {
        iter.seek(key);
        return iter.isValid() && Arrays.equals(key, iter.key());
    }

    static void getRow(RocksIterator iter,
                       byte[] row,
                       ColumnSelection columnSelection,
                       long timestamp,
                       @Output Map<Cell, Value> results) {
        iter.seek(getKey(row, timestamp - 1));
        byte[] col = null;
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!Arrays.equals(row, cellAndTs.lhSide.getRowName())) {
                return;
            }
            if (cellAndTs.rhSide >= timestamp ||
                    !columnSelection.contains(cellAndTs.lhSide.getColumnName()) ||
                    Arrays.equals(col, cellAndTs.lhSide.getColumnName())) {
                continue;
            }
            col = cellAndTs.lhSide.getColumnName();
            results.put(cellAndTs.lhSide, Value.create(iter.value(), cellAndTs.rhSide));
        }
    }

    static Value getCell(RocksIterator iter,
                         Cell cell,
                         long timestamp) {
        iter.seek(getKey(cell, timestamp - 1));
        if (iter.isValid()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (cellAndTs.lhSide.equals(cell)) {
                return Value.create(iter.value(), cellAndTs.rhSide);
            }
        }
        return null;
    }

    static Long getTimestamp(RocksIterator iter,
                             Cell cell,
                             long timestamp) {
        iter.seek(getKey(cell, timestamp - 1));
        if (iter.isValid()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (cellAndTs.lhSide.equals(cell)) {
                return cellAndTs.rhSide;
            }
        }
        return null;
    }

    static void getTimestamps(RocksIterator iter,
                              Cell cell,
                              long timestamp,
                              @Output Multimap<Cell, Long> results) {
        iter.seek(getKey(cell, timestamp - 1));
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!cell.equals(cellAndTs.lhSide)) {
                return;
            }
            results.put(cellAndTs.lhSide, cellAndTs.rhSide);
        }
    }

    static byte[] getKey(byte[] row,
                         long timeStamp) {
        return getKey(row, new byte[1], timeStamp);
    }

    static byte[] getKey(Cell cell,
                         long timeStamp) {
        return getKey(cell.getRowName(), cell.getColumnName(), timeStamp);
    }

    static byte[] getKey(byte[] row,
                         byte[] col,
                         long timeStamp) {
        Preconditions.checkArgument(EncodingUtils.sizeOfVarLong(row.length) <= 2);
        byte[] rowSize = EncodingUtils.encodeVarLong(row.length);
        ArrayUtils.reverse(rowSize);

        byte[] key = new byte[row.length + col.length + 8 + rowSize.length];
        ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN)
                .put(row)
                .put(col)
                .putLong(timeStamp)
                .put(rowSize);
        return key;
    }

    static Pair<Cell, Long> parseCellAndTs(byte[] key) {
        byte[] rowSizeBytes = new byte[2];
        rowSizeBytes[0] = key[key.length - 1];
        rowSizeBytes[1] = key[key.length - 2];

        int rowSize = (int) EncodingUtils.decodeVarLong(rowSizeBytes);
        int colEnd = key.length - 8 - EncodingUtils.sizeOfVarLong(rowSize);

        byte[] rowName = Arrays.copyOf(key, rowSize);
        byte[] colName = Arrays.copyOfRange(key, rowSize, colEnd);
        long ts = Longs.fromBytes(
                key[colEnd+0],
                key[colEnd+1],
                key[colEnd+2],
                key[colEnd+3],
                key[colEnd+4],
                key[colEnd+5],
                key[colEnd+6],
                key[colEnd+7]);

        return Pair.create(Cell.create(rowName, colName), ts);
    }

    static boolean isInRange(byte[] row, byte[] endRow) {
        return endRow.length == 0 || PtBytes.compareTo(row, endRow) < 0;
    }
}
