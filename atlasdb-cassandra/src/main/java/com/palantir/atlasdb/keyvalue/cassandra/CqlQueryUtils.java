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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public final class CqlQueryUtils {
    private CqlQueryUtils() {
        // utility class
    }

    public static CellWithTimestamp getCellFromKeylessRow(CqlRow row, byte[] key) {
        byte[] rowName = key;
        byte[] columnName = row.getColumns().get(0).getValue();
        long timestamp = extractTimestamp(row, 1);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    public static CellWithTimestamp getCellFromRow(CqlRow row) {
        byte[] rowName = row.getColumns().get(0).getValue();
        byte[] columnName = row.getColumns().get(1).getValue();
        long timestamp = extractTimestamp(row, 2);

        return CellWithTimestamp.of(Cell.create(rowName, columnName), timestamp);
    }

    public static long extractTimestamp(CqlRow row, int columnIndex) {
        byte[] flippedTimestampAsBytes = row.getColumns().get(columnIndex).getValue();
        return ~PtBytes.toLong(flippedTimestampAsBytes);
    }

    public static Arg<String> key(byte[] row) {
        return UnsafeArg.of("key", CassandraKeyValueServices.encodeAsHex(row));
    }

    public static Arg<String> column1(byte[] column) {
        return UnsafeArg.of("column1", CassandraKeyValueServices.encodeAsHex(column));
    }

    public static Arg<Long> column2(long invertedTimestamp) {
        return SafeArg.of("column2", invertedTimestamp);
    }

    public static Arg<Long> limit(long limit) {
        return SafeArg.of("limit", limit);
    }

    public static Arg<String> quotedTableName(TableReference tableRef) {
        String tableNameWithQuotes = "\"" + CassandraKeyValueServiceImpl.internalTableName(tableRef) + "\"";
        return LoggingArgs.customTableName(tableRef, tableNameWithQuotes);
    }

    public static List<CellWithTimestamp> getCells(Function<CqlRow,
            CellWithTimestamp> cellTsExtractor,
            CqlResult cqlResult) {
        return cqlResult.getRows()
                .stream()
                .map(cellTsExtractor)
                .collect(Collectors.toList());
    }
}
