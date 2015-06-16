// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.table.description.render;

import java.util.SortedSet;

import com.palantir.atlasdb.table.description.NamedColumnDescription;

public class NamedColumnRenderer extends Renderer {
    private final String NamedColumn;
    private final SortedSet<NamedColumnDescription> cols;

    public NamedColumnRenderer(Renderer parent, String tableName, SortedSet<NamedColumnDescription> cols) {
        super(parent);
        this.NamedColumn = tableName + "NamedColumn";
        this.cols = cols;
    }

    @Override
    protected void run() {
        _("public enum ", NamedColumn, " {"); {
            for (NamedColumnDescription col : cols) {
                _(Renderers.UPPER_CASE(col.getLongName()), " {"); {
                    _("@Override");
                    _("public byte[] getShortName() {"); {
                        _("return PtBytes.toCachedBytes(\"", col.getShortName(), "\");");
                    } _("}");
                } _("},");
            }
            replace(",", ";");
            _();
            _("public abstract byte[] getShortName();");
            _();
            _("public static Function<", NamedColumn, ", byte[]> toShortName() {"); {
                _("return new Function<", NamedColumn, ", byte[]>() {"); {
                    _("@Override");
                    _("public byte[] apply(", NamedColumn, " namedColumn) {"); {
                        _("return namedColumn.getShortName();");
                    } _("}");
                } _("};");
            } _("}");
        } _("}");
    }
}
