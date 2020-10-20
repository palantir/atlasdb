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
package com.palantir.atlasdb.table.description.render;

import com.palantir.atlasdb.table.description.ColumnValueDescription;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
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
        line("public static final class ", RowResult, " implements TypedRowResult {");
        {
            fields();
            line();
            staticFactories();
            line();
            constructors();
            line();
            getRowName();
            line();
            getColumnValues();
            line();
            getRowNameFun();
            line();
            getColumnValuesFun();
            line();
            renderToString();
        }
        line("}");
    }

    private void fields() {
        line("private final ", Row, " rowName;");
        line("private final ImmutableSet<", ColumnValue, "> columnValues;");
    }

    private void staticFactories() {
        line("public static ", RowResult, " of(RowResult<byte[]> rowResult) {");
        {
            line(Row, " rowName = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(rowResult.getRowName());");
            line(
                    "Set<",
                    ColumnValue,
                    "> columnValues = Sets.newHashSetWithExpectedSize(rowResult.getColumns().size());");
            line("for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {");
            {
                line(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                line(Value, " value = ", ColumnValue, ".hydrateValue(e.getValue());");
                line("columnValues.add(", ColumnValue, ".of(col, value));");
            }
            line("}");
            line("return new ", RowResult, "(rowName, ImmutableSet.copyOf(columnValues));");
        }
        line("}");
    }

    private void constructors() {
        line("private ", RowResult, "(", Row, " rowName, ImmutableSet<", ColumnValue, "> columnValues) {");
        {
            line("this.rowName = rowName;");
            line("this.columnValues = columnValues;");
        }
        line("}");
    }

    private void getRowName() {
        line("@Override");
        line("public ", Row, " getRowName() {");
        {
            line("return rowName;");
        }
        line("}");
    }

    private void getColumnValues() {
        line("public Set<", ColumnValue, "> getColumnValues() {");
        {
            line("return columnValues;");
        }
        line("}");
    }

    private void getRowNameFun() {
        line("public static Function<", RowResult, ", ", Row, "> getRowNameFun() {");
        {
            line("return new Function<", RowResult, ", ", Row, ">() {");
            {
                line("@Override");
                line("public ", Row, " apply(", RowResult, " rowResult) {");
                {
                    line("return rowResult.rowName;");
                }
                line("}");
            }
            line("};");
        }
        line("}");
    }

    private void getColumnValuesFun() {
        line("public static Function<", RowResult, ", ImmutableSet<", ColumnValue, ">> getColumnValuesFun() {");
        {
            line("return new Function<", RowResult, ", ImmutableSet<", ColumnValue, ">>() {");
            {
                line("@Override");
                line("public ImmutableSet<", ColumnValue, "> apply(", RowResult, " rowResult) {");
                {
                    line("return rowResult.columnValues;");
                }
                line("}");
            }
            line("};");
        }
        line("}");
    }

    private void renderToString() {
        line("@Override");
        line("public String toString() {");
        {
            line("return MoreObjects.toStringHelper(getClass().getSimpleName())");
            line("    .add(\"RowName\", getRowName())");
            line("    .add(\"ColumnValues\", getColumnValues())");
            line("    .toString();");
        }
        line("}");
    }
}
