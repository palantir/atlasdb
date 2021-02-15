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

import com.palantir.atlasdb.table.description.NamedColumnDescription;
import java.util.SortedSet;

public class NamedColumnRenderer extends Renderer {
    private final String namedColumn;
    private final SortedSet<NamedColumnDescription> cols;

    public NamedColumnRenderer(Renderer parent, String tableName, SortedSet<NamedColumnDescription> cols) {
        super(parent);
        this.namedColumn = tableName + "NamedColumn";
        this.cols = cols;
    }

    @Override
    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    protected void run() {
        line("public enum ", namedColumn, " {");
        {
            for (NamedColumnDescription col : cols) {
                line(Renderers.UPPER_CASE(col.getLongName()), " {");
                {
                    line("@Override");
                    line("public byte[] getShortName() {");
                    {
                        line("return PtBytes.toCachedBytes(\"", col.getShortName(), "\");");
                    }
                    line("}");
                }
                line("},");
            }
            replace(",", ";");
            line();
            line("public abstract byte[] getShortName();");
            line();
            line("public static Function<", namedColumn, ", byte[]> toShortName() {");
            {
                line("return new Function<", namedColumn, ", byte[]>() {");
                {
                    line("@Override");
                    line("public byte[] apply(", namedColumn, " namedColumn) {");
                    {
                        line("return namedColumn.getShortName();");
                    }
                    line("}");
                }
                line("};");
            }
            line("}");
        }
        line("}");
    }
}
