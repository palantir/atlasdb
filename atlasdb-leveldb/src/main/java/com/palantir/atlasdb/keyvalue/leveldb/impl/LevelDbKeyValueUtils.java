// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.leveldb.impl;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.iq80.leveldb.DBComparator;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.util.Pair;


final class LevelDbKeyValueUtils {
    private LevelDbKeyValueUtils() { /* */ }

    static DBComparator getComparator() {
        return new DBComparator() {

            @Override
            public int compare(byte[] o1, byte[] o2) {
                TableEntry key1 = parseKey(o1);
                TableEntry key2 = parseKey(o2);
                return new CompareToBuilder()
                .append(key1.tablePrefix, key2.tablePrefix, UnsignedBytes.lexicographicalComparator())
                .append(key1.cell, key2.cell)
                .append(key1.ts, key2.ts)
                .toComparison();
            }

            @Override
            public String name() {
                return "atlasdb comparator";
            }

            @Override
            public byte[] findShortestSeparator(byte[] start, byte[] limit) {
                return start;
            }

            @Override
            public byte[] findShortSuccessor(byte[] key) {
                return key;
            }
        };
    }

    static byte[] getKeyPrefixWithCell(byte[] tablePrefix, Cell cell) {
        return Bytes.concat(
                tablePrefix,
                cell.getRowName(),
                cell.getColumnName());
    }


    static byte[] getKeyPrefixWithRow(byte[] tablePrefix, byte[] row) {
        return Bytes.concat(tablePrefix, row);
    }

    static TableEntry parseKey(byte[] key) {
        int prefixIndex = com.google.common.primitives.Bytes.indexOf(key, (byte)',');
        if (prefixIndex + 1 == key.length) {
            return new TableEntry(key, null, 0);
        }
        byte[] prefixWithTable = PtBytes.head(key, prefixIndex+1);
        Pair<Cell, Long> parseCellAndTs = parseCellAndTs(key, prefixWithTable);
        return new TableEntry(prefixWithTable, parseCellAndTs.lhSide, parseCellAndTs.rhSide);
    }

    static class TableEntry {
        byte[] tablePrefix;
        Cell cell;
        long ts;

        public TableEntry(byte[] tablePrefix, Cell cell, long ts) {
            this.tablePrefix = tablePrefix;
            this.cell = cell;
            this.ts = ts;
        }
    }


    static Pair<Cell, Long> parseCellAndTs(byte[] key, byte[] prefixWithTable) {
        final byte[] rowAndColSize = PtBytes.tail(key, 4);
        ArrayUtils.reverse(rowAndColSize);

        final int rowSize = (int) EncodingUtils.decodeVarLong(rowAndColSize);
        final int colSize = (int)EncodingUtils.decodeVarLong(rowAndColSize, EncodingUtils.sizeOfVarLong(rowSize));

        final int rowStart = prefixWithTable.length;
        final int colStart = rowStart + rowSize;

        final byte[] rowName = ArrayUtils.subarray(key, rowStart, colStart);
        final byte[] colName = ArrayUtils.subarray(key, colStart, colStart + colSize);
        final long ts = EncodingUtils.decodeVarLong(key, colStart + colSize);
        return Pair.create(Cell.create(rowName, colName), ts);
    }


    static byte[] getKey(byte[] tablePrefix, Cell cell, long timeStamp) {
        Validate.isTrue(EncodingUtils.sizeOfVarLong(cell.getRowName().length) <= 2);
        Validate.isTrue(EncodingUtils.sizeOfVarLong(cell.getColumnName().length) <= 2);
        final byte[] rowSize = EncodingUtils.encodeVarLong(cell.getRowName().length);
        final byte[] colSize = EncodingUtils.encodeVarLong(cell.getColumnName().length);
        ArrayUtils.reverse(colSize);
        ArrayUtils.reverse(rowSize);
        return com.google.common.primitives.Bytes.concat(
                tablePrefix,
                cell.getRowName(),
                cell.getColumnName(),
                EncodingUtils.encodeVarLong(timeStamp),
                colSize,
                rowSize);
    }


    static boolean hasMovedPastPrefix(byte[] key, byte[] prefix) {
        return UnsignedBytes.lexicographicalComparator().compare(key, prefix) > 0
                && !PtBytes.startsWith(key, prefix);
    }


    static boolean isInRange(byte[] row, byte[] startRow, byte[] endRow) {
		return !(startRow.length != 0 && PtBytes.compareTo(row, startRow) < 0) &&
			   !(endRow.length != 0 && PtBytes.compareTo(row, endRow) >= 0);
	}
}
