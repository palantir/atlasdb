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

import static com.palantir.atlasdb.table.description.render.ColumnRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.VarName;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.short_name;

import java.util.SortedSet;

import com.palantir.atlasdb.table.description.NamedColumnDescription;

class NamedRowResultRenderer extends Renderer {
    private final String Row;
    private final String RowResult;
    private final SortedSet<NamedColumnDescription> cols;

    public NamedRowResultRenderer(Renderer parent, String name, SortedSet<NamedColumnDescription> cols) {
        super(parent);
        this.Row = name + "Row";
        this.RowResult = name + "RowResult";
        this.cols = cols;
    }

    @Override
    protected void run() {
        line("public static final class ", RowResult, " implements TypedRowResult {"); {
            fields();
            line();
            staticFactory();
            line();
            constructor();
            line();
            getRowName();
            line();
            getRowNameFun();
            line();
            fromRawRowResultFun();
            line();
            for (NamedColumnDescription col : cols) {
                hasCol(col);
                line();
            }
            for (NamedColumnDescription col : cols) {
                getCol(col);
                line();
            }
            for (NamedColumnDescription col : cols) {
                getColFun(col);
                line();
            }
            renderToString();
        } line("}");
    }

    private void fields() {
        line("private final RowResult<byte[]> row;");
    }

    private void staticFactory() {
        line("public static ", RowResult, " of(RowResult<byte[]> row) {"); {
            line("return new ", RowResult, "(row);");
        } line("}");
    }

    private void constructor() {
        line("private ", RowResult, "(RowResult<byte[]> row) {"); {
            line("this.row = row;");
        } line("}");
    }

    private void getRowName() {
        line("@Override");
        line("public ", Row, " getRowName() {"); {
            line("return ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());");
        } line("}");
    }

    private void getRowNameFun() {
        line("public static Function<", RowResult, ", ", Row, "> getRowNameFun() {"); {
            line("return new Function<", RowResult, ", ", Row, ">() {"); {
                line("@Override");
                line("public ", Row, " apply(", RowResult, " rowResult) {"); {
                    line("return rowResult.getRowName();");
                } line("}");
            } line("};");
        } line("}");
    }

    private void fromRawRowResultFun() {
        line("public static Function<RowResult<byte[]>, ", RowResult, "> fromRawRowResultFun() {"); {
            line("return new Function<RowResult<byte[]>, ", RowResult, ">() {"); {
                line("@Override");
                line("public ", RowResult, " apply(RowResult<byte[]> rowResult) {"); {
                    line("return new ", RowResult, "(rowResult);");
                } line("}");
            } line("};");
        } line("}");
    }

    private void hasCol(NamedColumnDescription col) {
        line("public boolean has", VarName(col), "() {"); {
            line("return row.getColumns().containsKey(PtBytes.toCachedBytes(", short_name(col), "));");
        } line("}");
    }

    private void getCol(NamedColumnDescription col) {
        line("public ", TypeName(col), " get", VarName(col), "() {"); {
            line("byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes(", short_name(col), "));");
            line("if (bytes == null) {"); {
                line("return null;");
            } line("}");
            line(Renderers.CamelCase(col.getLongName()), " value = ", Renderers.CamelCase(col.getLongName()), ".BYTES_HYDRATOR.hydrateFromBytes(bytes);");
            line("return value.getValue();");
        } line("}");
    }

    private void getColFun(NamedColumnDescription col) {
        line("public static Function<", RowResult, ", ", TypeName(col), "> get", VarName(col), "Fun() {"); {
            line("return new Function<", RowResult, ", ", TypeName(col), ">() {"); {
                line("@Override");
                line("public ", TypeName(col), " apply(", RowResult, " rowResult) {"); {
                    line("return rowResult.get", VarName(col), "();");
                } line("}");
            } line("};");
        } line("}");
    }

    private void renderToString() {
        line("@Override");
        line("public String toString() {"); {
            line("return MoreObjects.toStringHelper(getClass().getSimpleName())");
            line("    .add(\"RowName\", getRowName())");
            for (NamedColumnDescription col : cols) {
                line("    .add(\"", VarName(col), "\", get", VarName(col), "())");
            }
            line("    .toString();");
        } line("}");
    }
}
