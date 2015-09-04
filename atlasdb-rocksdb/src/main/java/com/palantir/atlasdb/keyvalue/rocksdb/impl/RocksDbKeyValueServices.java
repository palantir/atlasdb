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
        iter.seek(getKey(row, Value.INVALID_VALUE_TIMESTAMP));
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!Arrays.equals(row, cellAndTs.lhSide.getRowName())) {
                return;
            }
            if (cellAndTs.rhSide >= timestamp || !columnSelection.contains(cellAndTs.lhSide.getColumnName())) {
                continue;
            }
            results.put(cellAndTs.lhSide, Value.create(iter.value(), cellAndTs.rhSide));
        }
    }

    static Value getCell(RocksIterator iter,
                         Cell cell,
                         long timestamp) {
        Value value = null;
        iter.seek(getKey(cell, Value.INVALID_VALUE_TIMESTAMP));
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!cellAndTs.lhSide.equals(cell) || cellAndTs.rhSide >= timestamp) {
                break;
            }
            value = Value.create(iter.value(), cellAndTs.rhSide);
        }
        return value;
    }

    static Long getTimestamp(RocksIterator iter,
                             Cell cell,
                             long timestamp) {
        Long ts = null;
        iter.seek(getKey(cell, Value.INVALID_VALUE_TIMESTAMP));
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!cellAndTs.lhSide.equals(cell) || cellAndTs.rhSide >= timestamp) {
                break;
            }
            ts = cellAndTs.rhSide;
        }
        return ts;
    }

    static void getTimestamps(RocksIterator iter,
                              Cell cell,
                              long timestamp,
                              @Output Multimap<Cell, Long> results) {
        iter.seek(getKey(cell, Value.INVALID_VALUE_TIMESTAMP));
        for (; iter.isValid(); iter.next()) {
            Pair<Cell, Long> cellAndTs = parseCellAndTs(iter.key());
            if (!cell.equals(cellAndTs.lhSide) || cellAndTs.rhSide >= timestamp) {
                return;
            }
            results.put(cellAndTs.lhSide, cellAndTs.rhSide);
        }
    }

    static byte[] getKeyPrefix(Cell cell) {
        byte[] rowName = cell.getRowName();
        byte[] colName = cell.getColumnName();

        byte[] key = new byte[rowName.length + colName.length];
        System.arraycopy(rowName, 0, key, 0, rowName.length);
        System.arraycopy(colName, 0, key, rowName.length, colName.length);
        return key;
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
        byte[] rowSizeBytes = PtBytes.tail(key, 2);
        ArrayUtils.reverse(rowSizeBytes);

        int rowSize = (int) EncodingUtils.decodeVarLong(rowSizeBytes);
        int colSize = key.length - rowSize - 8 - EncodingUtils.sizeOfVarLong(rowSize);
        byte[] rowName = new byte[rowSize];
        if (colSize < 0) {
            System.out.println("bad");
        }
        byte[] colName = new byte[colSize];

        ByteBuffer keyBuffer = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        keyBuffer.get(rowName);
        keyBuffer.get(colName);
        long ts = keyBuffer.getLong();

        return Pair.create(Cell.create(rowName, colName), ts);
    }

    static boolean isInRange(byte[] row, byte[] endRow) {
        return endRow.length == 0 || PtBytes.compareTo(row, endRow) < 0;
    }
}
