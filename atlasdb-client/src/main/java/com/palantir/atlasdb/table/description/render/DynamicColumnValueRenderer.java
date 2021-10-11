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

import com.google.common.base.Splitter;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;

import static com.palantir.atlasdb.table.description.render.ComponentRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ComponentRenderers.varName;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class DynamicColumnValueRenderer extends Renderer {
    private final NameMetadataDescription col;
    private final ColumnValueDescription val;
    private final String Column;
    private final String ColumnValue;
    private final String Value;

    public DynamicColumnValueRenderer(Renderer parent, String tableName, DynamicColumnDescription dynamicCol) {
        super(parent);
        this.col = dynamicCol.getColumnNameDesc();
        this.val = dynamicCol.getValue();
        this.Column = tableName + "Column";
        this.ColumnValue = tableName + "ColumnValue";
        this.Value = val.getJavaObjectTypeName();
    }

    @Override
    protected void run() {
        javaDoc();
        line("public static final class ", ColumnValue, " implements ColumnValue<", Value, "> {");
        {
            fields();
            line();
            staticFactories();
            line();
            constructors();
            line();
            getColumnName();
            line();
            getValue();
            line();
            persistColumnName();
            line();
            persistValue();
            line();
            hydrateValue();
            line();
            getColumnNameFun();
            line();
            getValueFun();
            line();
            renderToString();
        }
        line("}");
    }

    private void javaDoc() {
        line("/**");
        line(" * <pre>");
        line(" * Column name description {", "");
        for (NameComponentDescription comp : col.getRowParts()) {
            boolean descending = comp.getOrder() == ValueByteOrder.DESCENDING;
            line(" *   {@literal ", descending ? "@Descending " : "", TypeName(comp), " ", varName(comp), "};");
        }
        line(" * }", "");
        line(" * Column value description {", "");
        line(" *   type: ", val.getJavaObjectTypeName(), ";");
        if (val.getProtoDescriptor() != null) {
            String protoDescription = val.getProtoDescriptor().toProto().toString();
            for (String line : Splitter.on('\n').split(protoDescription)) {
                line(" *   ", line, "");
            }
        }
        line(" * }", "");
        line(" * </pre>");
        line(" */");
    }

    private void fields() {
        line("private final ", Column, " columnName;");
        line("private final ", Value, " value;");
    }

    private void staticFactories() {
        line("public static ", ColumnValue, " of(", Column, " columnName, ", Value, " value) {");
        {
            line("return new ", ColumnValue, "(columnName, value);");
        }
        line("}");
    }

    private void constructors() {
        line("private ", ColumnValue, "(", Column, " columnName, ", Value, " value) {");
        {
            line("this.columnName = columnName;");
            line("this.value = value;");
        }
        line("}");
    }

    private void getColumnName() {
        line("public ", Column, " getColumnName() {");
        {
            line("return columnName;");
        }
        line("}");
    }

    private void getValue() {
        line("@Override");
        line("public ", Value, " getValue() {");
        {
            line("return value;");
        }
        line("}");
    }

    private void persistColumnName() {
        line("@Override");
        line("public byte[] persistColumnName() {");
        {
            line("return columnName.persistToBytes();");
        }
        line("}");
    }

    private void persistValue() {
        line("@Override");
        line("public byte[] persistValue() {");
        {
            switch (val.getFormat()) {
                case PERSISTABLE:
                    line("byte[] bytes = value.persistToBytes();");
                    break;
                case PROTO:
                    line("byte[] bytes = value.toByteArray();");
                    break;
                case PERSISTER:
                    line("byte[] bytes = ", val.getPersistCode("value"), ";");
                    break;
                case VALUE_TYPE:
                    line("byte[] bytes = ", val.getValueType().getPersistCode("value"), ";");
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported value type: " + val.getFormat());
            }
            line(
                    "return CompressionUtils.compress(bytes, Compression.",
                    val.getCompression().name(),
                    ");");
        }
        line("}");
    }

    private void hydrateValue() {
        line("public static ", Value, " hydrateValue(byte[] bytes) {");
        {
            line(
                    "bytes = CompressionUtils.decompress(bytes, Compression.",
                    val.getCompression().name(),
                    ");");
            switch (val.getFormat()) {
                case PERSISTABLE:
                    line("return ", Value, ".BYTES_HYDRATOR.hydrateFromBytes(bytes);");
                    break;
                case PROTO:
                    line("try {");
                    {
                        line("return ", Value, ".parseFrom(bytes);");
                    }
                    line("} catch (InvalidProtocolBufferException e) {");
                    {
                        line("throw Throwables.throwUncheckedException(e);");
                    }
                    line("}");
                    break;
                case PERSISTER:
                    line("return ", val.getHydrateCode("bytes"), ";");
                    break;
                case VALUE_TYPE:
                    line("return ", val.getValueType().getHydrateCode("bytes", "0"), ";");
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported value type: " + val.getFormat());
            }
        }
        line("}");

        if (val.isReusablePersister()) {
            line(val.getInstantiateReusablePersisterCode(true));
        }
    }

    private void getColumnNameFun() {
        line("public static Function<", ColumnValue, ", ", Column, "> getColumnNameFun() {");
        {
            line("return new Function<", ColumnValue, ", ", Column, ">() {");
            {
                line("@Override");
                line("public ", Column, " apply(", ColumnValue, " columnValue) {");
                {
                    line("return columnValue.getColumnName();");
                }
                line("}");
            }
            line("};");
        }
        line("}");
    }

    private void getValueFun() {
        line("public static Function<", ColumnValue, ", ", Value, "> getValueFun() {");
        {
            line("return new Function<", ColumnValue, ", ", Value, ">() {");
            {
                line("@Override");
                line("public ", Value, " apply(", ColumnValue, " columnValue) {");
                {
                    line("return columnValue.getValue();");
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
            line("    .add(\"ColumnName\", this.columnName)");
            line("    .add(\"Value\", this.value)");
            line("    .toString();");
        }
        line("}");
    }
}
