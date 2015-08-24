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

import static com.palantir.atlasdb.table.description.render.ComponentRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ComponentRenderers.varName;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;

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
        _("public static final class ", ColumnValue, " implements ColumnValue<", Value, "> {"); {
            fields();
            _();
            staticFactories();
            _();
            constructors();
            _();
            getColumnName();
            _();
            getValue();
            _();
            persistColumnName();
            _();
            persistValue();
            _();
            hydrateValue();
            _();
            getColumnNameFun();
            _();
            getValueFun();
        } _("}");
    }

    private void javaDoc() {
        _("/**");
        _(" * <pre>");
        _(" * Column name description {", "");
        for (NameComponentDescription comp : col.getRowParts()) {
            boolean descending = comp.getOrder() == ValueByteOrder.DESCENDING;
            _(" *   {@literal ", descending ? "@Descending " : "",  TypeName(comp), " ", varName(comp), "};");
        }
        _(" * }", "");
        _(" * Column value description {", "");
        _(" *   type: ", val.getJavaObjectTypeName(), ";");
        if (val.getProtoDescriptor() != null) {
            String protoDescription = val.getProtoDescriptor().toProto().toString();
            for (String line : protoDescription.split("\n")) {
                _(" *   ", line, "");
            }
        }
        _(" * }", "");
        _(" * </pre>");
        _(" */");
    }

    private void fields() {
        _("private final ", Column, " columnName;");
        _("private final ", Value, " value;");
    }

    private void staticFactories() {
        _("public static ", ColumnValue, " of(", Column, " columnName, ", Value, " value) {"); {
            _("return new ", ColumnValue, "(columnName, value);");
        } _("}");
    }

    private void constructors() {
        _("private ", ColumnValue, "(", Column, " columnName, ", Value, " value) {"); {
            _("this.columnName = columnName;");
            _("this.value = value;");
        } _("}");
    }

    private void getColumnName() {
        _("public ", Column, " getColumnName() {"); {
            _("return columnName;");
        } _("}");
    }

    private void getValue() {
        _("@Override");
        _("public ", Value, " getValue() {"); {
            _("return value;");
        } _("}");
    }

    private void persistColumnName() {
        _("@Override");
        _("public byte[] persistColumnName() {"); {
            _("return columnName.persistToBytes();");
        } _("}");
    }

    private void persistValue() {
        _("@Override");
        _("public byte[] persistValue() {"); {
            switch (val.getFormat()) {
            case PERSISTABLE:
                _("byte[] bytes = value.persistToBytes();");
                break;
            case PROTO:
                _("byte[] bytes = value.toByteArray();");
                break;
            case VALUE_TYPE:
                _("byte[] bytes = ", val.getValueType().getPersistCode("value"), ";");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + val.getFormat());
            }
            _("return CompressionUtils.compress(bytes, Compression.", val.getCompression().name(), ");");
        } _("}");
    }

    private void hydrateValue() {
        _("public static ", Value, " hydrateValue(byte[] bytes) {"); {
            _("bytes = CompressionUtils.decompress(bytes, Compression.", val.getCompression().name(), ");");
            switch (val.getFormat()) {
            case PERSISTABLE:
                _("return ", Value, ".BYTES_HYDRATOR.hydrateFromBytes(bytes);");
                break;
            case PROTO:
                _("try {"); {
                    _("return ", Value, ".parseFrom(bytes);");
                } _("} catch (InvalidProtocolBufferException e) {"); {
                    _("throw Throwables.throwUncheckedException(e);");
                } _("}");
                break;
            case VALUE_TYPE:
                _("return ", val.getValueType().getHydrateCode("bytes", "0"), ";");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + val.getFormat());
            }
        } _("}");
    }

    private void getColumnNameFun() {
        _("public static Function<", ColumnValue, ", ", Column, "> getColumnNameFun() {"); {
            _("return new Function<", ColumnValue, ", ", Column, ">() {"); {
                _("@Override");
                _("public ", Column, " apply(", ColumnValue, " columnValue) {"); {
                    _("return columnValue.getColumnName();");
                } _("}");
            } _("};");
        } _("}");
    }

    private void getValueFun() {
        _("public static Function<", ColumnValue, ", ", Value, "> getValueFun() {"); {
            _("return new Function<", ColumnValue, ", ", Value, ">() {"); {
                _("@Override");
                _("public ", Value, " apply(", ColumnValue, " columnValue) {"); {
                    _("return columnValue.getValue();");
                } _("}");
            } _("};");
        } _("}");
    }
}
