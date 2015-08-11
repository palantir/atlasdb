package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableCellDeserializer extends StdDeserializer<TableCell> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableCellDeserializer(TableMetadataCache metadataCache) {
        super(TableCell.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableCell deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        String tableName = node.get("table").textValue();
        TableMetadata metadata = metadataCache.getMetadata(tableName);
        Iterable<Cell> cells = AtlasDeserializers.deserializeCells(metadata, node.get("data"));
        return new TableCell(tableName, cells);
    }
}
