package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableCellSerializer extends StdSerializer<TableCell> {
    private static final long serialVersionUID = 1L;
    private final TableMetadataCache metadataCache;

    public TableCellSerializer(TableMetadataCache metadataCache) {
        super(TableCell.class);
        this.metadataCache = metadataCache;
    }

    @Override
    public void serialize(TableCell value,
                          JsonGenerator jgen,
                          SerializerProvider provider) throws IOException, JsonGenerationException {
        TableMetadata metadata = metadataCache.getMetadata(value.getTableName());
        Preconditions.checkNotNull(metadata, "Unknown table %s", value.getTableName());
        jgen.writeStartObject(); {
            jgen.writeStringField("table", value.getTableName());
            jgen.writeArrayFieldStart("data"); {
                for (Cell cell : value.getCells()) {
                    serialize(jgen, metadata, cell);
                }
            } jgen.writeEndArray();
        } jgen.writeEndObject();
    }

    private static void serialize(JsonGenerator jgen,
                                  TableMetadata metadata,
                                  Cell cell) throws IOException, JsonGenerationException {
        byte[] row = cell.getRowName();
        byte[] col = cell.getColumnName();

        jgen.writeStartObject(); {
            AtlasSerializers.serializeRow(jgen, metadata.getRowMetadata(), row);

            ColumnMetadataDescription columns = metadata.getColumns();
            if (columns.hasDynamicColumns()) {
                DynamicColumnDescription dynamicColumn = columns.getDynamicColumn();
                AtlasSerializers.serializeDynamicColumn(jgen, dynamicColumn, col);
            } else {
                String shortName = PtBytes.toString(col);
                jgen.writeStringField("col", getLongColumnName(columns, shortName));
            }
        } jgen.writeEndObject();
    }

    private static String getLongColumnName(ColumnMetadataDescription colDescription,
                                            String shortName) {
        for (NamedColumnDescription description : colDescription.getNamedColumns()) {
            if (shortName.equals(description.getShortName())) {
                return description.getLongName();
            }
        }
        throw new IllegalArgumentException("Unknown column with short name " + shortName);
    }
}
