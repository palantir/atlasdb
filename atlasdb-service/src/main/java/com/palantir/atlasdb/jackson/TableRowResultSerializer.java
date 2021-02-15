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
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class TableRowResultSerializer extends StdSerializer<TableRowResult> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    public TableRowResultSerializer(TableMetadataCache metadataCache) {
        super(TableRowResult.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public void serialize(TableRowResult value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        TableMetadata metadata = metadataCache.getMetadata(value.getTableName());
        Preconditions.checkNotNull(metadata, "Unknown table %s", value.getTableName());
        jgen.writeStartObject();
        jgen.writeStringField("table", value.getTableName());
        jgen.writeArrayFieldStart("data");
        for (RowResult<byte[]> result : value.getResults()) {
            serialize(jgen, metadata, result);
        }
        jgen.writeEndArray();
        jgen.writeEndObject();
    }

    private static void serialize(JsonGenerator jgen, TableMetadata metadata, RowResult<byte[]> result)
            throws IOException {
        jgen.writeStartObject();
        AtlasSerializers.serializeRow(jgen, metadata.getRowMetadata(), result.getRowName());
        ColumnMetadataDescription columns = metadata.getColumns();
        if (columns.hasDynamicColumns()) {
            jgen.writeArrayFieldStart("cols");
            for (Map.Entry<byte[], byte[]> colVal : result.getColumns().entrySet()) {
                jgen.writeStartObject();
                byte[] col = colVal.getKey();
                byte[] val = colVal.getValue();
                DynamicColumnDescription dynamicColumn = columns.getDynamicColumn();
                AtlasSerializers.serializeDynamicColumn(jgen, dynamicColumn, col);
                jgen.writeFieldName("val");
                AtlasSerializers.serializeVal(jgen, dynamicColumn.getValue(), val);
                jgen.writeEndObject();
            }
            jgen.writeEndArray();
        } else {
            jgen.writeObjectFieldStart("cols");
            SortedMap<byte[], byte[]> columnValues = result.getColumns();
            Set<NamedColumnDescription> namedColumns = columns.getNamedColumns();
            for (NamedColumnDescription description : namedColumns) {
                byte[] col = PtBytes.toCachedBytes(description.getShortName());
                byte[] val = columnValues.get(col);
                if (val != null) {
                    AtlasSerializers.serializeNamedCol(jgen, description, val);
                }
            }
            jgen.writeEndObject();
        }
        jgen.writeEndObject();
    }
}
