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
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.swing.table.AbstractTableModel;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.collect.IterableView;
import com.palantir.ptoss.util.Throwables;
import com.palantir.util.Pair;

public class AtlasShellDynamicColumnTableModel extends AbstractTableModel {
    private static final long serialVersionUID = 1L;

    private final TableMetadata tableMetadata;
    private final List<Map.Entry<Cell, byte[]>> values;

    /**
     * Cache for rendered cell values. Rendering can be a little CPU intensive for large values, so
     * we cache the results for zippiness.
     */
    private final LoadingCache<Pair<Integer, Integer>, Object> valueAtCache = CacheBuilder.newBuilder().maximumSize(
            1000).build(new CacheLoader<Pair<Integer, Integer>, Object>() {
        @Override
        public Object load(Pair<Integer, Integer> key) throws Exception {
            int rowIndex = key.getLhSide();
            switch (key.getRhSide()) {
            case 0:
                byte[] rowName = values.get(rowIndex).getKey().getRowName();
                return tableMetadata.getRowMetadata().renderToJson(rowName);
            case 1:
                byte[] columnName = values.get(rowIndex).getKey().getColumnName();
                return tableMetadata.getColumns().getDynamicColumn().getColumnNameDesc().renderToJson(
                        columnName);
            case 2:
                byte[] value = values.get(rowIndex).getValue();
                return AtlasShellRubyHelpers.formatColumnValue(
                        tableMetadata.getColumns().getDynamicColumn().getValue(),
                        value);
            default:
                throw new IllegalArgumentException("Invalid cell.");
            }
        }
    });

    public AtlasShellDynamicColumnTableModel(TableMetadata tableMetadata,
                                             List<RowResult<byte[]>> rows) {
        Preconditions.checkArgument(tableMetadata.getColumns().hasDynamicColumns());
        this.tableMetadata = tableMetadata;
        this.values = Lists.newArrayList();
        for (RowResult<byte[]> row : rows) {
            values.addAll(IterableView.of(row.getCells()).immutableCopy());
        }
    }

    @Override
    public int getRowCount() {
        return values.size();
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        return false;
    }

    @Override
    public int getColumnCount() {
        return 3;
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
        switch (column) {
        case 0:
            return "row_name";
        case 1:
            return "col_name";
        case 2:
            return "value";
        default:
            throw new IllegalArgumentException("Invalid cell.");
        }
    }
}
