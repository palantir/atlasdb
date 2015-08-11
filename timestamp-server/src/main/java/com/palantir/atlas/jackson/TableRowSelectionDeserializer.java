package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Iterables;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableRowSelectionDeserializer extends StdDeserializer<TableRowSelection> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableRowSelectionDeserializer(TableMetadataCache metadataCache) {
        super(TableRowSelection.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableRowSelection deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
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
