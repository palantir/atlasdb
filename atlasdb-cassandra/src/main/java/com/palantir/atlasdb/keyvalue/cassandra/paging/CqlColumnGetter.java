/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.thrift.TException;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.AllCellsPerRowPager;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;
import com.palantir.common.base.Throwables;
import com.palantir.util.paging.Pager;

public class CqlColumnGetter implements ColumnGetter {
    private final TableReference tableRef;
    private final int columnBatchSize;
    private final CqlExecutor cqlExecutor;

    public CqlColumnGetter(CqlExecutor cqlExecutor, TableReference tableRef, int columnBatchSize) {
        this.cqlExecutor = cqlExecutor;
        this.tableRef = tableRef;
        this.columnBatchSize = columnBatchSize;
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> getColumnsByRow(List<KeySlice> firstPage) {
        Set<ByteBuffer> rows = getRowsFromPage(firstPage);
        try {
            return getColumnsByRow(rows);
        } catch (TException ex) {
            throw Throwables.throwUncheckedException(ex);
        }
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getColumnsByRow(Set<ByteBuffer> rows) throws TException {
        return rows.stream().collect(Collectors.toMap(
                Function.identity(),
                this::getColumns));
    }

    private List<ColumnOrSuperColumn> getColumns(ByteBuffer row) {
        AllCellsPerRowPager allCellsPerRowPager = new AllCellsPerRowPager(
                cqlExecutor,
                row,
                tableRef,
                columnBatchSize);
        return new Pager<>(allCellsPerRowPager).getPages();
    }

    private Set<ByteBuffer> getRowsFromPage(List<KeySlice> firstPage) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = ColumnGetters.getColsByKey(firstPage);
        return colsByKey.keySet();
    }
}
