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
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;

public class TableCellSerializer extends StdSerializer<TableCell> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    public TableCellSerializer(TableMetadataCache metadataCache) {
        super(TableCell.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public void serialize(TableCell value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        TableMetadata metadata = metadataCache.getMetadata(value.getTableName());
        Preconditions.checkNotNull(metadata, "Unknown table %s", value.getTableName());
        jgen.writeStartObject();
        jgen.writeStringField("table", value.getTableName());
        jgen.writeArrayFieldStart("data");
        for (Cell cell : value.getCells()) {
            serialize(jgen, metadata, cell);
        }
        jgen.writeEndArray();
        jgen.writeEndObject();
    }

    private static void serialize(JsonGenerator jgen, TableMetadata metadata, Cell cell) throws IOException {
        byte[] row = cell.getRowName();
        byte[] col = cell.getColumnName();

        jgen.writeStartObject();
        AtlasSerializers.serializeRow(jgen, metadata.getRowMetadata(), row);
        ColumnMetadataDescription columns = metadata.getColumns();
        if (columns.hasDynamicColumns()) {
            DynamicColumnDescription dynamicColumn = columns.getDynamicColumn();
            AtlasSerializers.serializeDynamicColumn(jgen, dynamicColumn, col);
        } else {
            String shortName = PtBytes.toString(col);
            jgen.writeStringField("col", getLongColumnName(columns, shortName));
        }
        jgen.writeEndObject();
    }

    private static String getLongColumnName(ColumnMetadataDescription colDescription, String shortName) {
        for (NamedColumnDescription description : colDescription.getNamedColumns()) {
            if (shortName.equals(description.getShortName())) {
                return description.getLongName();
            }
        }
        throw new IllegalArgumentException("Unknown column with short name " + shortName);
    }
}
