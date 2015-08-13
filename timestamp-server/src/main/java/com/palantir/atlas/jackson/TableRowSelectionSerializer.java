package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableRowSelectionSerializer extends StdSerializer<TableRowSelection> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    public TableRowSelectionSerializer(TableMetadataCache metadataCache) {
        super(TableRowSelection.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public void serialize(TableRowSelection value,
                          JsonGenerator jgen,
                          SerializerProvider provider) throws IOException, JsonGenerationException {
        TableMetadata metadata = metadataCache.getMetadata(value.getTableName());
        Preconditions.checkNotNull(metadata, "Unknown table %s", value.getTableName());
        jgen.writeStartObject(); {
            jgen.writeStringField("table", value.getTableName());
            jgen.writeArrayFieldStart("rows"); {
                for (byte[] row : value.getRows()) {
                    AtlasSerializers.serializeRowish(jgen, metadata.getRowMetadata(), row);
                }
            } jgen.writeEndArray();
            if (!value.getColumnSelection().allColumnsSelected()) {
                jgen.writeArrayFieldStart("cols"); {
                    for (byte[] col : value.getColumnSelection().getSelectedColumns()) {
                        jgen.writeUTF8String(col, 0, col.length);
                    }
                } jgen.writeEndArray();
            }
        } jgen.writeEndObject();
    }
}
