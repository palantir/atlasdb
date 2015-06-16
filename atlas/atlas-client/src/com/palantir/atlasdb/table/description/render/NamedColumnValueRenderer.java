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

import static com.palantir.atlasdb.table.description.render.ColumnRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.long_name;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.short_name;

import com.palantir.atlasdb.table.description.NamedColumnDescription;

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
        _("public static final class ", Name, " implements ", tableName, "NamedColumnValue<", TypeName(col), "> {"); {
            fields();
            _();
            staticFactories();
            _();
            constructors();
            _();
            getColumnName();
            _();
            getShortColumnName();
            _();
            getValue();
            _();
            persistValue();
            _();
            persistColumnName();
            _();
            bytesHydrator();
        } _("}");
    }

    private void javaDoc() {
        _("/**");
        _(" * <pre>");
        _(" * Column value description {", "");
        _(" *   type: ", TypeName(col), ";");
        if (col.getValue().getProtoDescriptor() != null) {
            String protoDescription = col.getValue().getProtoDescriptor().toProto().toString();
            for (String line : protoDescription.split("\n")) {
                _(" *   ", line, "");
            }
        }
        _(" * }", "");
        _(" * </pre>");
        _(" */");
    }

    private void fields() {
        _("private final ", TypeName(col), " value;");
    }

    private void staticFactories() {
        _("public static ", Name, " of(", TypeName(col), " value) {"); {
            _("return new ", Name, "(value);");
        } _("}");
    }

    private void constructors() {
        _("private ", Name, "(", TypeName(col), " value) {"); {
            _("this.value = value;");
        } _("}");
    }

    private void getColumnName() {
        _("@Override");
        _("public String getColumnName() {"); {
            _("return ", long_name(col), ";");
        } _("}");
    }

    private void getShortColumnName() {
        _("@Override");
        _("public String getShortColumnName() {"); {
            _("return ", short_name(col), ";");
        } _("}");
    }

    private void getValue() {
        _("@Override");
        _("public ", TypeName(col), " getValue() {"); {
            _("return value;");
        } _("}");
    }

    private void persistValue() {
        _("@Override");
        _("public byte[] persistValue() {"); {
            switch (col.getValue().getFormat()) {
            case PERSISTABLE:
                _("byte[] bytes = value.persistToBytes();");
                break;
            case PROTO:
                _("byte[] bytes = value.toByteArray();");
                break;
            case VALUE_TYPE:
                _("byte[] bytes = ", col.getValue().getValueType().getPersistCode("value"), ";");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + col.getValue().getFormat());
            }
            _("return CompressionUtils.compress(bytes, Compression.", col.getValue().getCompression().name(), ");");
        } _("}");
    }

    private void persistColumnName() {
        _("@Override");
        _("public byte[] persistColumnName() {"); {
            _("return PtBytes.toCachedBytes(", short_name(col), ");");
        } _("}");
    }

    private void bytesHydrator() {
        _("public static final Hydrator<", Name, "> BYTES_HYDRATOR = new Hydrator<", Name, ">() {"); {
            _("@Override");
            _("public ", Name, " hydrateFromBytes(byte[] bytes) {"); {
                _("bytes = CompressionUtils.decompress(bytes, Compression.", col.getValue().getCompression().name(), ");");
                switch (col.getValue().getFormat()) {
                case PERSISTABLE:
                    _("return of(", TypeName(col), ".BYTES_HYDRATOR.hydrateFromBytes(bytes));");
                    break;
                case PROTO:
                    _("try {"); {
                        _("return of(", TypeName(col), ".parseFrom(bytes));");
                    } _("} catch (InvalidProtocolBufferException e) {"); {
                        _("throw Throwables.throwUncheckedException(e);");
                    } _("}");
                    break;
                case VALUE_TYPE:
                    _("return of(", col.getValue().getValueType().getHydrateCode("bytes", "0"), ");");
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported value type: " + col.getValue().getFormat());
                }
            } _("}");
        } _("};");
    }
}
