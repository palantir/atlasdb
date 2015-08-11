package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableRangeDeserializer extends StdDeserializer<TableRange> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    protected TableRangeDeserializer(TableMetadataCache metadataCache) {
        super(TableRange.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public TableRange deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.readValueAsTree();
        String tableName = node.get("table").textValue();
        TableMetadata metadata = metadataCache.getMetadata(tableName);
        JsonNode optBatchSize = node.get("batch_size");
        int batchSize = optBatchSize == null ? 2000 : optBatchSize.asInt();
        Iterable<byte[]> columns = AtlasDeserializers.deserializeNamedCols(metadata.getColumns(), node.get("cols"));
        byte[] startRow = new byte[0];
        byte[] endRow = new byte[0];
        if (node.has("prefix")) {
            startRow = AtlasDeserializers.deserializeRowPrefix(metadata.getRowMetadata(), node.get("prefix"));
            endRow = RangeRequests.createEndNameForPrefixScan(startRow);
        } else {
            if (node.has("raw_start")) {
                startRow = node.get("raw_start").binaryValue();
            } else if (node.has("start")) {
                startRow = AtlasDeserializers.deserializeRow(metadata.getRowMetadata(), node.get("start"));
            }
            if (node.has("raw_end")) {
                endRow = node.get("raw_end").binaryValue();
            } else if (node.has("end")) {
                endRow = AtlasDeserializers.deserializeRow(metadata.getRowMetadata(), node.get("end"));
            }
        }
        return new TableRange(tableName, startRow, endRow, columns, batchSize);
    }
}
