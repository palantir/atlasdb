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
package com.palantir.atlasdb.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;

public class TableMetadataSerializer extends StdSerializer<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected TableMetadataSerializer() {
        super(TableMetadata.class, false);
    }

    @Override
    public void serialize(TableMetadata value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        NameMetadataDescription rowMetadata = value.getRowMetadata();
        ColumnMetadataDescription columnMetadata = value.getColumns();
        boolean isDynamic = columnMetadata.hasDynamicColumns();
        jgen.writeBooleanField("is_dynamic", isDynamic);
        jgen.writeArrayFieldStart("row");
        serialize(jgen, rowMetadata);
        jgen.writeEndArray();
        if (isDynamic) {
            DynamicColumnDescription dynamicColumn = columnMetadata.getDynamicColumn();
            jgen.writeArrayFieldStart("column");
            serialize(jgen, dynamicColumn.getColumnNameDesc());
            jgen.writeEndArray();
            jgen.writeObjectFieldStart("value");
            serialize(jgen, dynamicColumn.getValue());
            jgen.writeEndObject();
        } else {
            jgen.writeArrayFieldStart("columns");

            for (NamedColumnDescription namedColumn : columnMetadata.getNamedColumns()) {
                jgen.writeStartObject();
                jgen.writeObjectField("name", namedColumn.getShortName());
                jgen.writeObjectField("long_name", namedColumn.getLongName());
                jgen.writeObjectFieldStart("value");
                serialize(jgen, namedColumn.getValue());
                jgen.writeEndObject();
                jgen.writeEndObject();
            }
            jgen.writeEndArray();
        }
        jgen.writeStringField("sweepStrategy", value.getSweepStrategy().name());
        jgen.writeStringField("conflictHandler", value.getConflictHandler().name());
        jgen.writeStringField("cachePriority", value.getCachePriority().name());
        jgen.writeBooleanField("rangeScanAllowed", value.isRangeScanAllowed());
        jgen.writeNumberField("explicitCompressionBlockSizeKB", value.getExplicitCompressionBlockSizeKB());
        jgen.writeBooleanField("negativeLookups", value.hasNegativeLookups());
        jgen.writeBooleanField("appendHeavyAndReadLight", value.isAppendHeavyAndReadLight());
        jgen.writeStringField("nameLogSafety", value.getNameLogSafety().name());
        jgen.writeEndObject();
    }

    private void serialize(JsonGenerator jgen, NameMetadataDescription rowMetadata) throws IOException {
        for (NameComponentDescription rowPart : rowMetadata.getRowParts()) {
            jgen.writeStartObject();
            jgen.writeObjectField("name", rowPart.getComponentName());
            jgen.writeObjectField("type", rowPart.getType());
            jgen.writeObjectField("order", rowPart.getOrder());
            jgen.writeEndObject();
        }
    }

    private void serialize(JsonGenerator jgen, ColumnValueDescription value) throws IOException {
        jgen.writeObjectField("format", value.getFormat());
        switch (value.getFormat()) {
            case PERSISTABLE:
            case PERSISTER:
                jgen.writeObjectField("type", value.getJavaObjectTypeName());
                break;
            case PROTO:
                jgen.writeObjectFieldStart("type");
                serialize(jgen, value.getProtoDescriptor());
                jgen.writeEndObject();
                break;
            case VALUE_TYPE:
                jgen.writeObjectField("type", value.getValueType());
                break;
            default:
                throw new EnumConstantNotPresentException(Format.class, value.getFormat().name());

        }
    }

    private void serialize(JsonGenerator jgen, Descriptor descriptor) throws IOException {
        jgen.writeObjectField("name", descriptor.getName());
        jgen.writeArrayFieldStart("fields");
        for (FieldDescriptor field : descriptor.getFields()) {
            jgen.writeStartObject();
            serialize(jgen, field);
            jgen.writeEndObject();
        }
        jgen.writeEndArray();
    }

    private void serialize(JsonGenerator jgen, FieldDescriptor field) throws IOException {
        jgen.writeObjectField("name", field.getName());
        jgen.writeObjectField("label", field.toProto().getLabel());
        jgen.writeObjectField("type_name", field.getType());
        if (field.getType() == Type.MESSAGE) {
            jgen.writeObjectFieldStart("type");
            serialize(jgen, field.getMessageType());
            jgen.writeEndObject();
        } else if (field.getType() == Type.ENUM) {
            jgen.writeObjectFieldStart("type");
            serialize(jgen, field.getEnumType());
            jgen.writeEndObject();
        }
    }

    private void serialize(JsonGenerator jgen, EnumDescriptor enumType) throws IOException {
        jgen.writeObjectField("name", enumType.getName());
        jgen.writeArrayFieldStart("values");
        for (EnumValueDescriptor value : enumType.getValues()) {
            jgen.writeString(value.getName());
        }
        jgen.writeEndArray();
    }
}
