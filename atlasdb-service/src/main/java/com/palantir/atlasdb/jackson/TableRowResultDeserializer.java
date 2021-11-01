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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class TableRowResultDeserializer extends StdDeserializer<TableRowResult> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableRowResultDeserializer(TableMetadataCache metadataCache) {
        super(TableRowResult.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableRowResult deserialize(JsonParser jp, DeserializationContext _ctxt) throws IOException {
        JsonNode node = jp.readValueAsTree();
        String tableName = node.get("table").textValue();
        Collection<RowResult<byte[]>> rowResults = new ArrayList<>();
        TableMetadata metadata = metadataCache.getMetadata(tableName);
        for (JsonNode rowResult : node.get("data")) {
            byte[] row = AtlasDeserializers.deserializeRow(metadata.getRowMetadata(), rowResult.get("row"));
            ImmutableSortedMap.Builder<byte[], byte[]> cols =
                    ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
            if (metadata.getColumns().hasDynamicColumns()) {
                for (JsonNode colVal : rowResult.get("cols")) {
                    byte[] col = AtlasDeserializers.deserializeCol(metadata.getColumns(), colVal.get("col"));
                    byte[] val = AtlasDeserializers.deserializeVal(
                            metadata.getColumns().getDynamicColumn().getValue(), colVal.get("val"));
                    cols.put(col, val);
                }
            } else {
                JsonNode namedCols = rowResult.get("cols");
                for (NamedColumnDescription namedCol : metadata.getColumns().getNamedColumns()) {
                    JsonNode valNode = namedCols.get(namedCol.getLongName());
                    if (valNode != null) {
                        byte[] col = namedCol.getShortName().getBytes(StandardCharsets.UTF_8);
                        byte[] val = AtlasDeserializers.deserializeVal(namedCol.getValue(), valNode);
                        cols.put(col, val);
                    }
                }
            }
            rowResults.add(RowResult.create(row, cols.build()));
        }
        return new TableRowResult(tableName, rowResults);
    }
}
