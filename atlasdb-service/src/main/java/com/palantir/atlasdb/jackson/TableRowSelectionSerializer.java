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
import com.palantir.atlasdb.api.TableRowSelection;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;

public class TableRowSelectionSerializer extends StdSerializer<TableRowSelection> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    public TableRowSelectionSerializer(TableMetadataCache metadataCache) {
        super(TableRowSelection.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public void serialize(TableRowSelection value, JsonGenerator jgen, SerializerProvider _provider)
            throws IOException {
        TableMetadata metadata = metadataCache.getMetadata(value.getTableName());
        Preconditions.checkNotNull(metadata, "Unknown table %s", value.getTableName());
        jgen.writeStartObject();
        jgen.writeStringField("table", value.getTableName());
        jgen.writeArrayFieldStart("rows");
        for (byte[] row : value.getRows()) {
            AtlasSerializers.serializeRowish(jgen, metadata.getRowMetadata(), row);
        }
        jgen.writeEndArray();
        if (!value.getColumnSelection().allColumnsSelected()) {
            jgen.writeArrayFieldStart("cols");
            for (byte[] col : value.getColumnSelection().getSelectedColumns()) {
                jgen.writeUTF8String(col, 0, col.length);
            }
            jgen.writeEndArray();
        }
        jgen.writeEndObject();
    }
}
