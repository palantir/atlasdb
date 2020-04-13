/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.api;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.StoredValue;
import com.palantir.atlasdb.v2.api.api.ScanFilter.ColumnsFilter;

public final class GetOperations {
    private GetOperations() {}

    public static NewGetOperation<Map<Cell, StoredValue>> getCells(NewIds.Table table, Set<Cell> cells) {
        return new NewGetOperation<Map<Cell, StoredValue>>() {
            @Override
            public NewIds.Table table() {
                return table;
            }

            @Override
            public ScanFilter scanFilter() {
                return ScanFilter.cells(cells);
            }

            @Override
            public ResultBuilder<Map<Cell, StoredValue>> newResultBuilder() {
                return new ResultBuilder<Map<Cell, StoredValue>>() {
                    private final ImmutableMap.Builder<Cell, StoredValue> resultBuilder = new ImmutableMap.Builder<>();

                    @Override
                    public ShouldContinue add(NewIds.Table table, Cell cell, StoredValue value) {
                        resultBuilder.put(cell, value);
                        return ShouldContinue.YES;
                    }

                    @Override
                    public Map<Cell, StoredValue> build() {
                        return resultBuilder.build();
                    }
                };
            }
        };
    }

    public static NewGetOperation<Map<Row, NewRowResult>> getRows(
            NewIds.Table table, Set<Row> rows, ColumnsFilter filter) {
        return new NewGetOperation<Map<Row, NewRowResult>>() {
            @Override
            public NewIds.Table table() {
                return table;
            }

            @Override
            public ScanFilter scanFilter() {
                return ScanFilter.rowsAndColumns(ScanFilter.exactRows(ImmutableSortedSet.copyOf(rows)), filter, -1);
            }

            @Override
            public ResultBuilder<Map<Row, NewRowResult>> newResultBuilder() {
                return new ResultBuilder<Map<Row, NewRowResult>>() {
                    private final ImmutableMap.Builder<Row, NewRowResult> result = new ImmutableMap.Builder<>();
                    private Row currentRow;
                    private NewRowResult.Builder currentColumns;

                    @Override
                    public ShouldContinue add(NewIds.Table table, Cell cell, StoredValue value) {
                        if (currentRow == null) {
                            currentRow = cell.row();
                            currentColumns = new NewRowResult.Builder();
                        } else if (!currentRow.equals(cell.row())) {
                            result.put(currentRow, currentColumns.build());
                            currentRow = cell.row();
                            currentColumns = new NewRowResult.Builder();
                        } else {
                            currentColumns.putColumns(cell.column(), value);
                        }
                        return ShouldContinue.YES;
                    }

                    @Override
                    public Map<Row, NewRowResult> build() {
                        if (currentRow != null) {
                            result.put(currentRow, currentColumns.build());
                        }
                        return result.build();
                    }
                };
            }
        };
    }
}
