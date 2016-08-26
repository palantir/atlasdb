/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.util.Pair;
import com.palantir.util.paging.PageGetter;

public class AllCellsPerRowPager implements PageGetter<ColumnOrSuperColumn> {
    private CqlExecutor cqlExecutor;
    private String row;
    private TableReference tableRef;
    private int pageSize;

    public AllCellsPerRowPager(CqlExecutor executor, ByteBuffer row, TableReference tableRef, int pageSize) {
        this.cqlExecutor = executor;
        this.row = CassandraKeyValueServices.encodeAsHex(row.array());
        this.tableRef = tableRef;
        this.pageSize = pageSize;
    }

    @Override
    public List<ColumnOrSuperColumn> getFirstPage() {
        CqlResult cqlResult = cqlExecutor.getColumnsForRow(tableRef, row, pageSize);
        return getColumns(cqlResult);
    }

    @Override
    public List<ColumnOrSuperColumn> getNextPage(List<ColumnOrSuperColumn> currentPage) {
        ColumnOrSuperColumn previousResult = Iterables.getLast(currentPage);

        Pair<byte[], Long> nameAndTimestamp = CassandraKeyValueServices.decomposeName(previousResult.getColumn());
        String columnName = CassandraKeyValueServices.encodeAsHex(nameAndTimestamp.getLhSide());
        long timestamp = ~nameAndTimestamp.getRhSide();

        CqlResult cqlResult = cqlExecutor.getTimestampsForRowAndColumn(tableRef, row, columnName, timestamp, pageSize);
        List<ColumnOrSuperColumn> columns = getColumns(cqlResult);

        if (columns.size() < pageSize) {
            // We finished with this column, but there might be more, so let's capture them
            CqlResult secondCqlResult =
                    cqlExecutor.getNextColumnsForRow(tableRef, row, columnName, pageSize - columns.size());
            columns.addAll(getColumns(secondCqlResult));
        }

        return columns;
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    private List<ColumnOrSuperColumn> getColumns(CqlResult cqlResult) {
        return cqlResult.getRows().stream()
                .map(this::getColumn)
                .collect(Collectors.toList());
    }

    private ColumnOrSuperColumn getColumn(CqlRow cqlRow) {
        byte[] columnName = cqlRow.getColumns().get(0).getValue();
        byte[] timestampAsBytes = cqlRow.getColumns().get(1).getValue();

        return CassandraKeyValueServices.makeColumnOrSuperColumn(columnName, timestampAsBytes);
    }
}
