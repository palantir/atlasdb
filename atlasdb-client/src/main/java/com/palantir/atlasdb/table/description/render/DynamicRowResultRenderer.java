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
package com.palantir.atlasdb.table.description.render;

import com.palantir.atlasdb.table.description.ColumnValueDescription;


class DynamicRowResultRenderer extends Renderer {
    private final String Row;
    private final String Column;
    private final String Value;
    private final String ColumnValue;
    private final String RowResult;

    public DynamicRowResultRenderer(Renderer parent, String tableName, ColumnValueDescription val) {
        super(parent);
        this.Row = tableName + "Row";
        this.Column = tableName + "Column";
        this.Value = val.getJavaObjectTypeName();
        this.ColumnValue = tableName + "ColumnValue";
        this.RowResult = tableName + "RowResult";
    }

    @Override
    protected void run() {
        _("public static final class ", RowResult, " implements TypedRowResult {"); {
            fields();
            _();
            staticFactories();
            _();
            constructors();
            _();
            getRowName();
            _();
            getColumnValues();
            _();
            getRowNameFun();
            _();
            getColumnValuesFun();
        } _("}");
    }

    private void fields() {
        _("private final ", Row, " rowName;");
        _("private final ImmutableSet<", ColumnValue, "> columnValues;");
    }

    private void staticFactories() {
        _("public static ", RowResult, " of(RowResult<byte[]> rowResult) {"); {
            _(Row, " rowName = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());");
            _("Set<", ColumnValue, "> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());");
            _("for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {"); {
                _(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                _(Value, " value = ", ColumnValue, ".hydrateValue(e.getValue());");
                _("columnValues.add(", ColumnValue, ".of(col, value));");
            } _("}");
            _("return new ", RowResult, "(rowName, ImmutableSet.copyOf(columnValues));");
        } _("}");
    }

    private void constructors() {
        _("private ", RowResult, "(", Row, " rowName, ImmutableSet<", ColumnValue, "> columnValues) {"); {
            _("this.rowName = rowName;");
            _("this.columnValues = columnValues;");
        } _("}");
    }

    private void getRowName() {
        _("@Override");
        _("public ", Row, " getRowName() {"); {
            _("return rowName;");
        } _("}");
    }

    private void getColumnValues() {
        _("public Set<", ColumnValue, "> getColumnValues() {"); {
            _("return columnValues;");
        } _("}");
    }

    private void getRowNameFun() {
        _("public static Function<", RowResult, ", ", Row, "> getRowNameFun() {"); {
            _("return new Function<", RowResult, ", ", Row, ">() {"); {
                _("@Override");
                _("public ", Row, " apply(", RowResult, " rowResult) {"); {
                    _("return rowResult.rowName;");
                } _("}");
            } _("};");
        } _("}");
    }

    private void getColumnValuesFun() {
        _("public static Function<", RowResult, ", ImmutableSet<", ColumnValue, ">> getColumnValuesFun() {"); {
            _("return new Function<", RowResult, ", ImmutableSet<", ColumnValue, ">>() {"); {
                _("@Override");
                _("public ImmutableSet<", ColumnValue, "> apply(", RowResult, " rowResult) {"); {
                    _("return rowResult.columnValues;");
                } _("}");
            } _("};");
        } _("}");
    }
}
