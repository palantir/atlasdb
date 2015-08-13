package com.palantir.atlas.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlas.api.RangeToken;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowResult;

public class RangeTokenDeserializer extends StdDeserializer<RangeToken> {
    private static final long serialVersionUID = 1L;

    protected RangeTokenDeserializer() {
        super(TableCell.class);
    }

    @Override
    public RangeToken deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonToken t = jp.getCurrentToken();
        if (t == JsonToken.START_OBJECT) {
            t = jp.nextToken();
        }
        TableRowResult results = null;
        TableRange nextRange = null;
        for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            t = jp.nextToken();
            if (fieldName.equals("data")) {
                results = jp.readValueAs(TableRowResult.class);
            } else if (fieldName.equals("next")) {
                nextRange = jp.readValueAs(TableRange.class);
            }
        }
        return new RangeToken(results, nextRange);
    }
}
