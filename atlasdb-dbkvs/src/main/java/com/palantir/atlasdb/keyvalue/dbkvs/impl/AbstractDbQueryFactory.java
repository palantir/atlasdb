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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.Joiner;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractDbQueryFactory implements DbQueryFactory {
    @Override
    public FullQuery getRowsColumnRangeQuery(
            Map<byte[], BatchColumnRangeSelection> columnRangeSelectionsByRow,
            long ts) {
        List<String> subQueries = new ArrayList<>(columnRangeSelectionsByRow.size());
        int totalArgs = 0;
        for (BatchColumnRangeSelection columnRangeSelection : columnRangeSelectionsByRow.values()) {
            totalArgs += 2 + ((columnRangeSelection.getStartCol().length > 0) ? 1 : 0)
                    + ((columnRangeSelection.getEndCol().length > 0) ? 1 : 0);
        }
        List<Object> args = new ArrayList<>(totalArgs);
        for (Map.Entry<byte[], BatchColumnRangeSelection> entry : columnRangeSelectionsByRow.entrySet()) {
            FullQuery query = getRowsColumnRangeSubQuery(entry.getKey(), ts, entry.getValue());
            subQueries.add(query.getQuery());
            for (Object arg : query.getArgs()) {
                args.add(arg);
            }
        }
        String query = Joiner.on(") UNION ALL (").appendTo(new StringBuilder("("), subQueries).append(")")
                .append(" ORDER BY row_name ASC, col_name ASC").toString();
        return new FullQuery(query).withArgs(args);
    }

    @Override
    public FullQuery getRowsColumnRangeQuery(RowsColumnRangeBatchRequest batch, long ts) {
        List<FullQuery> fullQueries = new ArrayList<>();
        batch.getPartialFirstRow()
                .ifPresent(entry -> fullQueries.add(getRowsColumnRangeSubQuery(entry.getKey(), ts, entry.getValue())));
        if (!batch.getRowsToLoadFully().isEmpty()) {
            fullQueries.add(getRowsColumnRangeFullyLoadedRowsSubQuery(batch.getRowsToLoadFully(),
                    ts,
                    batch.getColumnRangeSelection()));

        }
        batch.getPartialLastRow()
                .ifPresent(entry -> fullQueries.add(getRowsColumnRangeSubQuery(entry.getKey(), ts, entry.getValue())));

        List<String> subQueries = fullQueries.stream().map(FullQuery::getQuery).collect(Collectors.toList());
        int totalArgs = fullQueries.stream().mapToInt(fullQuery -> fullQuery.getArgs().length).sum();
        List<Object> args = fullQueries.stream()
                .flatMap(fullQuery -> Stream.of(fullQuery.getArgs()))
                .collect(Collectors.toCollection(() -> new ArrayList<>(totalArgs)));
        String query = Joiner.on(") UNION ALL (")
                .appendTo(new StringBuilder("("), subQueries)
                .append(")")
                .append(" ORDER BY row_name ASC, col_name ASC")
                .toString();
        return new FullQuery(query).withArgs(args);
    }

    protected abstract FullQuery getRowsColumnRangeFullyLoadedRowsSubQuery(
            List<byte[]> rowsToLoadFully,
            long ts,
            ColumnRangeSelection columnRangeSelection);

    protected abstract FullQuery getRowsColumnRangeSubQuery(byte[] key, long ts, BatchColumnRangeSelection value);
}
