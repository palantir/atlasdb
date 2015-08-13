package com.palantir.atlas.jackson;

import java.io.IOException;

import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.palantir.atlas.api.RangeToken;

public class RangeTokenSerializer extends StdSerializer<RangeToken> {
    private static final long serialVersionUID = 1L;

    @Inject
    public RangeTokenSerializer() {
        super(RangeToken.class);
    }

    @Override
    public void serialize(RangeToken value,
                          JsonGenerator jgen,
                          SerializerProvider provider) throws IOException, JsonGenerationException {
        jgen.writeStartObject(); {
            jgen.writeObjectField("data", value.getResults());
            if (value.hasMoreResults()) {
                jgen.writeObjectField("next", value.getNextRange());
            }
        } jgen.writeEndObject();
    }
}
