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

import static com.palantir.atlasdb.table.description.render.ColumnRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.long_name;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.short_name;

import com.google.common.base.Splitter;
import com.palantir.atlasdb.table.description.NamedColumnDescription;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class NamedColumnValueRenderer extends Renderer {
    private final String tableName;
    private final String Name;
    private final NamedColumnDescription col;

    public NamedColumnValueRenderer(Renderer parent, String tableName, NamedColumnDescription col) {
        super(parent);
        this.tableName = tableName;
        this.Name = Renderers.CamelCase(col.getLongName());
        this.col = col;
    }

    @Override
    protected void run() {
        javaDoc();
        line("public static final class ", Name, " implements ", tableName, "NamedColumnValue<", TypeName(col), "> {");
        {
            fields();
            line();
            staticFactories();
            line();
            constructors();
            line();
            getColumnName();
            line();
            getShortColumnName();
            line();
            getValue();
            line();
            persistValue();
            line();
            persistColumnName();
            line();
            bytesHydrator();
            line();
            renderToString();
        }
        line("}");
    }

    private void javaDoc() {
        line("/**");
        line(" * <pre>");
        line(" * Column value description {", "");
        line(" *   type: ", TypeName(col), ";");
        if (col.getValue().getProtoDescriptor() != null) {
            String protoDescription =
                    col.getValue().getProtoDescriptor().toProto().toString();
            for (String line : Splitter.on('\n').split(protoDescription)) {
                line(" *   ", line, "");
            }
        }
        line(" * }", "");
        line(" * </pre>");
        line(" */");
    }

    private void fields() {
        line("private final ", TypeName(col), " value;");
    }

    private void staticFactories() {
        line("public static ", Name, " of(", TypeName(col), " value) {");
        {
            line("return new ", Name, "(value);");
        }
        line("}");
    }

    private void constructors() {
        line("private ", Name, "(", TypeName(col), " value) {");
        {
            line("this.value = value;");
        }
        line("}");
    }

    private void getColumnName() {
        line("@Override");
        line("public String getColumnName() {");
        {
            line("return ", long_name(col), ";");
        }
        line("}");
    }

    private void getShortColumnName() {
        line("@Override");
        line("public String getShortColumnName() {");
        {
            line("return ", short_name(col), ";");
        }
        line("}");
    }

    private void getValue() {
        line("@Override");
        line("public ", TypeName(col), " getValue() {");
        {
            line("return value;");
        }
        line("}");
    }

    private void persistValue() {
        line("@Override");
        line("public byte[] persistValue() {");
        {
            switch (col.getValue().getFormat()) {
                case PERSISTABLE:
                    line("byte[] bytes = value.persistToBytes();");
                    break;
                case PROTO:
                    line("byte[] bytes = value.toByteArray();");
                    break;
                case PERSISTER:
                    line("byte[] bytes = ", col.getValue().getPersistCode("value"), ";");
                    break;
                case VALUE_TYPE:
                    line("byte[] bytes = ", col.getValue().getValueType().getPersistCode("value"), ";");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported value type: " + col.getValue().getFormat());
            }
            line(
                    "return CompressionUtils.compress(bytes, Compression.",
                    col.getValue().getCompression().name(),
                    ");");
        }
        line("}");
    }

    private void persistColumnName() {
        line("@Override");
        line("public byte[] persistColumnName() {");
        {
            line("return PtBytes.toCachedBytes(", short_name(col), ");");
        }
        line("}");
    }

    private void bytesHydrator() {
        line("public static final Hydrator<", Name, "> BYTES_HYDRATOR = new Hydrator<", Name, ">() {");
        {
            line("@Override");
            line("public ", Name, " hydrateFromBytes(byte[] bytes) {");
            {
                line(
                        "bytes = CompressionUtils.decompress(bytes, Compression.",
                        col.getValue().getCompression().name(),
                        ");");
                switch (col.getValue().getFormat()) {
                    case PERSISTABLE:
                        line("return of(", TypeName(col), ".BYTES_HYDRATOR.hydrateFromBytes(bytes));");
                        break;
                    case PROTO:
                        line("try {");
                        {
                            line("return of(", TypeName(col), ".parseFrom(bytes));");
                        }
                        line("} catch (InvalidProtocolBufferException e) {");
                        {
                            line("throw Throwables.throwUncheckedException(e);");
                        }
                        line("}");
                        break;
                    case PERSISTER:
                        line("return of(", col.getValue().getHydrateCode("bytes"), ");");
                        break;
                    case VALUE_TYPE:
                        line("return of(", col.getValue().getValueType().getHydrateCode("bytes", "0"), ");");
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported value type: " + col.getValue().getFormat());
                }
            }
            line("}");

            if (col.getValue().isReusablePersister()) {
                line(col.getValue().getInstantiateReusablePersisterCode(false));
            }
        }
        line("};");
    }

    private void renderToString() {
        line("@Override");
        line("public String toString() {");
        {
            line("return MoreObjects.toStringHelper(getClass().getSimpleName())");
            line("    .add(\"Value\", this.value)");
            line("    .toString();");
        }
        line("}");
    }
}
