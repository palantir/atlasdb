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
package com.palantir.atlasdb.shell;

import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.swing.table.AbstractTableModel;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.ptoss.util.Throwables;
import com.palantir.util.Pair;

public class AtlasShellNamedColumnTableModel extends AbstractTableModel {
    private static final long serialVersionUID = 1L;

    private final TableMetadata tableMetadata;
    private final List<RowResult<byte[]>> rows;
    private final List<NamedColumnDescription> columns;
    private final LoadingCache<Pair<Integer, Integer>, Object> valueAtCache = CacheBuilder.newBuilder().maximumSize(
            1000).build(new CacheLoader<Pair<Integer, Integer>, Object>() {
        @Override
        public Object load(Pair<Integer, Integer> key) throws Exception {
            int columnIndex = key.getRhSide();
            RowResult<byte[]> row = rows.get(key.getLhSide());
            if (columnIndex == 0) {
                return tableMetadata.getRowMetadata().renderToJson(row.getRowName());
            } else {
                NamedColumnDescription column = columns.get(columnIndex - 1);
                String result = AtlasShellRubyHelpers.formatNamedColumnValue(column, row);
                return result != null ? result : "<null>";
            }
        }
    });

    public AtlasShellNamedColumnTableModel(final TableMetadata tableMetadata, List<String> columns, List<RowResult<byte[]>> rows) {
        Preconditions.checkArgument(!tableMetadata.getColumns().hasDynamicColumns());
        this.tableMetadata = tableMetadata;
        this.rows = rows;
        if (columns == null || columns.isEmpty()) {
            this.columns = ImmutableList.copyOf(tableMetadata.getColumns().getNamedColumns());
        } else {
            this.columns = ImmutableList.copyOf(Collections2.transform(columns, new Function<String, NamedColumnDescription>() {
                @Override
                public NamedColumnDescription apply(String columnName) {
                    for (NamedColumnDescription nc : tableMetadata.getColumns().getNamedColumns()) {
                        if (nc.getLongName().equals(columnName) || nc.getShortName().equals(columnName)) {
                            return nc;
                        }
                    }
                    throw new IllegalArgumentException("Unknown column " + columnName);
                }}));
        }
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return false;
    }

    @Override
    public int getColumnCount() {
        return columns.size() + 1;
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        try {
            return valueAtCache.get(Pair.create(rowIndex, columnIndex));
        } catch (ExecutionException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public String getColumnName(int column) {
        if (column == 0) {
            return "row_name";
        } else {
            return columns.get(column - 1).getLongName();
        }
    }
}
