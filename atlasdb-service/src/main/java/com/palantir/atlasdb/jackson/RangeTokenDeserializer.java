/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TableRowResult;
import java.io.IOException;

public class RangeTokenDeserializer extends StdDeserializer<RangeToken> {
    private static final long serialVersionUID = 1L;

    protected RangeTokenDeserializer() {
        super(TableCell.class);
    }

    @Override
    public RangeToken deserialize(JsonParser jp, DeserializationContext _ctxt) throws IOException {
        JsonToken jsonToken = jp.getCurrentToken();
        if (jsonToken == JsonToken.START_OBJECT) {
            jsonToken = jp.nextToken();
        }
        TableRowResult results = null;
        TableRange nextRange = null;
        for (; jsonToken == JsonToken.FIELD_NAME; jsonToken = jp.nextToken()) {
            String fieldName = jp.getCurrentName();
            jsonToken = jp.nextToken();
            if (fieldName.equals("data")) {
                results = jp.readValueAs(TableRowResult.class);
            } else if (fieldName.equals("next")) {
                nextRange = jp.readValueAs(TableRange.class);
            }
        }
        return new RangeToken(results, nextRange);
    }
}
