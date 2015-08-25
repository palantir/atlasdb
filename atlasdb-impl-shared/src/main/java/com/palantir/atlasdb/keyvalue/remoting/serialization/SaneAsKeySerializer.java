package com.palantir.atlasdb.keyvalue.remoting.serialization;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * This class simply converts any object to Json using the usual method and then
 * string-escapes it to make it a valid Json field name.
 *
 * @author htarasiuk
 *
 */
public final class SaneAsKeySerializer extends JsonSerializer<Object> {

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