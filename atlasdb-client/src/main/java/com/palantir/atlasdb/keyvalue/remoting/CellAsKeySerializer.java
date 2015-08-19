package com.palantir.atlasdb.keyvalue.remoting;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.palantir.atlasdb.keyvalue.api.Cell;

final class CellAsKeySerializer extends JsonSerializer<Cell> {

    private final ObjectMapper mapper = new ObjectMapper();
    private CellAsKeySerializer() { }
    private static final CellAsKeySerializer instance = new CellAsKeySerializer();
    public static CellAsKeySerializer instance() {
        return instance;
    }

    @Override
    public void serialize(Cell value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException, JsonProcessingException {
        gen.writeFieldName(mapper.writeValueAsString(value));
    }
}