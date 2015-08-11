package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.Iterables;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlasdb.encoding.PtBytes;

public class TableRangeSerializer extends StdSerializer<TableRange> {

    public TableRangeSerializer() {
        super(TableRange.class);
    }

    @Override
    public void serialize(TableRange value,
                          JsonGenerator jgen,
                          SerializerProvider provider) throws IOException, JsonGenerationException {
        jgen.writeStartObject(); {
            jgen.writeStringField("table", value.getTableName());
            jgen.writeBinaryField("raw_start", value.getStartRow());
            jgen.writeBinaryField("raw_end", value.getEndRow());
            jgen.writeNumberField("batch_size", value.getBatchSize());
            if (!Iterables.isEmpty(value.getColumns())) {
                jgen.writeArrayFieldStart("cols"); {
                    for (byte[] column : value.getColumns()) {
                        jgen.writeString(PtBytes.toString(column));
                    }
                } jgen.writeEndArray();
            }
        } jgen.writeEndObject();
    }
}
