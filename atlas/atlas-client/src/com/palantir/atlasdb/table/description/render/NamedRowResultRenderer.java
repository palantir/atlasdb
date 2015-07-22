// Copyright 2015 Palantir Technologies
//
// Licensed under the BSD-3 License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://opensource.org/licenses/BSD-3-Clause
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        _("public static final class ", RowResult, " implements TypedRowResult {"); {
            fields();
            _();
            staticFactory();
            _();
            constructor();
            _();
            getRowName();
            _();
            getRowNameFun();
            _();
            fromRawRowResultFun();
            _();
            for (NamedColumnDescription col : cols) {
                hasCol(col);
                _();
            }
            for (NamedColumnDescription col : cols) {
                getCol(col);
                _();
            }
            for (NamedColumnDescription col : cols) {
                getColFun(col);
                _();
            }
            renderToString();
        } _("}");
    }

    private void fields() {
        _("private final RowResult<byte[]> row;");
    }

    private void staticFactory() {
        _("public static ", RowResult, " of(RowResult<byte[]> row) {"); {
            _("return new ", RowResult, "(row);");
        } _("}");
    }

    private void constructor() {
        _("private ", RowResult, "(RowResult<byte[]> row) {"); {
            _("this.row = row;");
        } _("}");
    }

    private void getRowName() {
        _("@Override");
        _("public ", Row, " getRowName() {"); {
            _("return ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(row.getRowName());");
        } _("}");
    }

    private void getRowNameFun() {
        _("public static Function<", RowResult, ", ", Row, "> getRowNameFun() {"); {
            _("return new Function<", RowResult, ", ", Row, ">() {"); {
                _("@Override");
                _("public ", Row, " apply(", RowResult, " rowResult) {"); {
                    _("return rowResult.getRowName();");
                } _("}");
            } _("};");
        } _("}");
    }

    private void fromRawRowResultFun() {
        _("public static Function<RowResult<byte[]>, ", RowResult, "> fromRawRowResultFun() {"); {
            _("return new Function<RowResult<byte[]>, ", RowResult, ">() {"); {
                _("@Override");
                _("public ", RowResult, " apply(RowResult<byte[]> rowResult) {"); {
                    _("return new ", RowResult, "(rowResult);");
                } _("}");
            } _("};");
        } _("}");
    }

    private void hasCol(NamedColumnDescription col) {
        _("public boolean has", VarName(col), "() {"); {
            _("return row.getColumns().containsKey(PtBytes.toCachedBytes(", short_name(col), "));");
        } _("}");
    }

    private void getCol(NamedColumnDescription col) {
        _("public ", TypeName(col), " get", VarName(col), "() {"); {
            _("byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes(", short_name(col), "));");
            _("if (bytes == null) {"); {
                _("return null;");
            } _("}");
            _(Renderers.CamelCase(col.getLongName()), " value = ", Renderers.CamelCase(col.getLongName()), ".BYTES_HYDRATOR.hydrateFromBytes(bytes);");
            _("return value.getValue();");
        } _("}");
    }

    private void getColFun(NamedColumnDescription col) {
        _("public static Function<", RowResult, ", ", TypeName(col), "> get", VarName(col), "Fun() {"); {
            _("return new Function<", RowResult, ", ", TypeName(col), ">() {"); {
                _("@Override");
                _("public ", TypeName(col), " apply(", RowResult, " rowResult) {"); {
                    _("return rowResult.get", VarName(col), "();");
                } _("}");
            } _("};");
        } _("}");
    }

    private void renderToString() {
        _("@Override");
        _("public String toString() {"); {
            _("return MoreObjects.toStringHelper(this)");
            _("        .add(\"RowName\", getRowName())");
            for (NamedColumnDescription col : cols) {
                _("        .add(\"", VarName(col), "\", get", VarName(col), "())");
            }
            _("    .toString();");
        } _("}");
    }
}
