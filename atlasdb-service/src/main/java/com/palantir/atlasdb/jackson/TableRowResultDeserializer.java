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
package com.palantir.atlasdb.jackson;

import java.io.IOException;
import java.util.Collection;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableRowResultDeserializer extends StdDeserializer<TableRowResult> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableRowResultDeserializer(TableMetadataCache metadataCache) {
        super(TableRowResult.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableRowResult deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        String tableName = node.get("table").textValue();
        Collection<RowResult<byte[]>> rowResults = Lists.newArrayList();
        TableMetadata metadata = metadataCache.getMetadata(tableName);
        for (JsonNode rowResult : node.get("data")) {
            byte[] row = AtlasDeserializers.deserializeRow(metadata.getRowMetadata(), rowResult.get("row"));
            ImmutableSortedMap.Builder<byte[], byte[]> cols = ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
            if (metadata.getColumns().hasDynamicColumns()) {
                for (JsonNode colVal : rowResult.get("cols")) {
                    byte[] col = AtlasDeserializers.deserializeCol(metadata.getColumns(), colVal.get("col"));
                    byte[] val = AtlasDeserializers.deserializeVal(metadata.getColumns().getDynamicColumn().getValue(), colVal.get("val"));
                    cols.put(col, val);
                }
            } else {
                for (NamedColumnDescription namedCol : metadata.getColumns().getNamedColumns()) {
                    JsonNode valNode = rowResult.get(namedCol.getLongName());
                    if (valNode != null) {
                        byte[] col = namedCol.getShortName().getBytes(Charsets.UTF_8);
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
