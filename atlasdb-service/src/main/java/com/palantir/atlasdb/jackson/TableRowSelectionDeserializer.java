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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.api.TableRowSelection;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;

public class TableRowSelectionDeserializer extends StdDeserializer<TableRowSelection> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableRowSelectionDeserializer(TableMetadataCache metadataCache) {
        super(TableRowSelection.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableRowSelection deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonNode node = jp.readValueAsTree();
        String tableName = node.get("table").textValue();
        TableMetadata metadata = metadataCache.getMetadata(tableName);
        Iterable<byte[]> rows = AtlasDeserializers.deserializeRows(metadata.getRowMetadata(), node.get("rows"));
        Iterable<byte[]> columns = AtlasDeserializers.deserializeNamedCols(metadata.getColumns(), node.get("cols"));
        if (Iterables.isEmpty(columns)) {
            return new TableRowSelection(tableName, rows, ColumnSelection.all());
        } else {
            return new TableRowSelection(tableName, rows, ColumnSelection.create(columns));
        }
    }
}
