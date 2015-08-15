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
package com.palantir.atlas.jackson;

import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.util.Pair;

public class AtlasSerializers {

    private AtlasSerializers() {
        // cannot instantiate
    }

    public static void serializeRow(JsonGenerator jgen,
                                    NameMetadataDescription rowDescription,
                                    byte[] row) throws IOException, JsonGenerationException {
        jgen.writeFieldName("row");
        serializeRowish(jgen, rowDescription, row);
    }

    public static void serializeDynamicColumn(JsonGenerator jgen,
                                              DynamicColumnDescription colDescription,
                                              byte[] col) throws IOException, JsonGenerationException {
        jgen.writeFieldName("col");
        serializeRowish(jgen, colDescription.getColumnNameDesc(), col);
    }

    public static void serializeRowish(JsonGenerator jgen,
                                       NameMetadataDescription rowDescription,
                                       byte[] row) throws IOException, JsonGenerationException {
        int offset = 0;
        byte[] flippedRow = null;
        jgen.writeStartArray(); {
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
                jgen.writeRawValue(parse.getLhSide());
                offset += parse.getRhSide();
            }
        } jgen.writeEndArray();
    }

    public static void serializeNamedCol(JsonGenerator jgen,
                                         NamedColumnDescription description,
                                         byte[] val) throws IOException, JsonGenerationException {
        jgen.writeFieldName(description.getLongName());
        serializeVal(jgen, description.getValue(), val);
    }

    public static ColumnValueDescription serializeCol(JsonGenerator jgen,
                                                      ColumnMetadataDescription colDescription,
                                                      byte[] col) throws IOException, JsonGenerationException {
        if (colDescription.hasDynamicColumns()) {
            DynamicColumnDescription dynMetadata = colDescription.getDynamicColumn();
            NameMetadataDescription description = dynMetadata.getColumnNameDesc();
            jgen.writeRawValue(description.renderToJson(col));
            return dynMetadata.getValue();
        } else {
            jgen.writeString(PtBytes.toString(col));
            Set<NamedColumnDescription> namedColumns = colDescription.getNamedColumns();
            for (NamedColumnDescription description : namedColumns) {
                if (UnsignedBytes.lexicographicalComparator().compare(col,
                        PtBytes.toCachedBytes(description.getShortName())) == 0) {
                    return description.getValue();
                }
            }
            throw new IllegalArgumentException("Column " +  BaseEncoding.base16().lowerCase().encode(col) + " is not a valid column.");
        }
    }

    public static void serializeVal(JsonGenerator jgen,
                                    ColumnValueDescription description,
                                    byte[] val) throws IOException, JsonGenerationException {
        switch (description.getFormat()) {
        case BLOCK_STORED_PROTO:
            jgen.writeBinary(val);
            break;
        case PERSISTABLE:
            jgen.writeBinary(val);
            break;
        case PROTO:
            Message proto = description.hydrateProto(AtlasSerializers.class.getClassLoader(), val);
//            String rawJson = ForkedJsonFormat.printToString(proto);
            String rawJson = JsonFormat.printToString(proto);
            jgen.writeRawValue(rawJson);
            break;
        case VALUE_TYPE:
            String parsedValue = description.getValueType().convertToJson(val);
            jgen.writeRawValue(parsedValue);
            break;
        default:
            throw new EnumConstantNotPresentException(Format.class, description.getFormat().name());
        }
    }
}
