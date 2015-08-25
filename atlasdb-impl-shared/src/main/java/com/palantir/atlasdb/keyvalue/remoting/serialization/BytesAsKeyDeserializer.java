package com.palantir.atlasdb.keyvalue.remoting.serialization;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class BytesAsKeyDeserializer extends KeyDeserializer {

    private final ObjectMapper mapper = new ObjectMapper();
    private final static BytesAsKeyDeserializer instance = new BytesAsKeyDeserializer();

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        return mapper.readValue(key, byte[].class);
    }

    public static BytesAsKeyDeserializer instance() {
        return instance;
    }

}
