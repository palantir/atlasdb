package com.palantir.atlasdb.keyvalue.remoting;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

final class SaneAsKeySerializer extends JsonSerializer<Object> {

    private final ObjectMapper mapper = new ObjectMapper();
    private SaneAsKeySerializer() { }
    private static final SaneAsKeySerializer instance = new SaneAsKeySerializer();
    public static SaneAsKeySerializer instance() {
        return instance;
    }

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException, JsonProcessingException {
        gen.writeFieldName(mapper.writeValueAsString(value));
    }
}