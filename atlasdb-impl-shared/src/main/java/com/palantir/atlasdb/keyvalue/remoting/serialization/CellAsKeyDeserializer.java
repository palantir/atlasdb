package com.palantir.atlasdb.keyvalue.remoting.serialization;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.keyvalue.api.Cell;

public final class CellAsKeyDeserializer extends KeyDeserializer {

    private final ObjectMapper mapper = new ObjectMapper();
    private CellAsKeyDeserializer() { }
    private static final CellAsKeyDeserializer instance = new CellAsKeyDeserializer();
    public static CellAsKeyDeserializer instance() {
        return instance;
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        return mapper.readValue(key, Cell.class);
    }
}