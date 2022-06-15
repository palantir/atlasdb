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
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.Message;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.proto.fork.ForkedJsonFormat;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.util.Pair;
import java.io.IOException;
import java.util.Set;

public final class AtlasSerializers {

    private AtlasSerializers() {
        // cannot instantiate
    }

    public static void serializeRow(JsonGenerator jgen, NameMetadataDescription rowDescription, byte[] row)
            throws IOException {
        jgen.writeFieldName("row");
        serializeRowish(jgen, rowDescription, row);
    }

    public static void serializeDynamicColumn(JsonGenerator jgen, DynamicColumnDescription colDescription, byte[] col)
            throws IOException {
        jgen.writeFieldName("col");
        serializeRowish(jgen, colDescription.getColumnNameDesc(), col);
    }

    public static void serializeRowish(JsonGenerator jgen, NameMetadataDescription rowDescription, byte[] row)
            throws IOException {
        int offset = 0;
        byte[] flippedRow = null;
        jgen.writeStartObject();
        for (NameComponentDescription part : rowDescription.getRowParts()) {
            if (part.isReverseOrder() && flippedRow == null) {
                flippedRow = EncodingUtils.flipAllBits(row);
            }
            Pair<String, Integer> parse;
            if (part.isReverseOrder()) {
                parse = part.getType().convertToJson(flippedRow, offset);
            } else {
                parse = part.getType().convertToJson(row, offset);
            }
            jgen.writeFieldName(part.getComponentName());
            jgen.writeRawValue(parse.getLhSide());
            offset += parse.getRhSide();
        }
        jgen.writeEndObject();
    }

    public static void serializeNamedCol(JsonGenerator jgen, NamedColumnDescription description, byte[] val)
            throws IOException {
        jgen.writeFieldName(description.getLongName());
        serializeVal(jgen, description.getValue(), val);
    }

    public static void serializeCol(JsonGenerator jgen, ColumnMetadataDescription colDescription, byte[] col)
            throws IOException {
        if (colDescription.hasDynamicColumns()) {
            DynamicColumnDescription dynMetadata = colDescription.getDynamicColumn();
            NameMetadataDescription description = dynMetadata.getColumnNameDesc();
            jgen.writeRawValue(description.renderToJson(col));
        } else {
            Set<NamedColumnDescription> namedColumns = colDescription.getNamedColumns();
            for (NamedColumnDescription description : namedColumns) {
                if (UnsignedBytes.lexicographicalComparator()
                                .compare(col, PtBytes.toCachedBytes(description.getShortName()))
                        == 0) {
                    jgen.writeString(description.getLongName());
                    return;
                }
            }
            throw new IllegalArgumentException(
                    "Column " + BaseEncoding.base16().lowerCase().encode(col) + " is not a valid column.");
        }
    }

    public static void serializeVal(JsonGenerator jgen, ColumnValueDescription description, byte[] val)
            throws IOException {
        switch (description.getFormat()) {
            case PERSISTABLE:
            case PERSISTER:
                jgen.writeBinary(val);
                break;
            case PROTO:
                Message proto = description.hydrateProto(AtlasSerializers.class.getClassLoader(), val);
                String rawJson = ForkedJsonFormat.printToString(proto);
                jgen.writeRawValue(rawJson);
                break;
            case VALUE_TYPE:
                String parsedValue = description.getValueType().convertToJson(val);
                jgen.writeRawValue(parsedValue);
                break;
            default:
                throw new EnumConstantNotPresentException(
                        Format.class, description.getFormat().name());
        }
    }
}
