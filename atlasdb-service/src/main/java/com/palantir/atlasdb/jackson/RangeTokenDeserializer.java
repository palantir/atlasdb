/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.jackson;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TableRowResult;

public class RangeTokenDeserializer extends StdDeserializer<RangeToken> {
    private static final long serialVersionUID = 1L;

    protected RangeTokenDeserializer() {
        super(TableCell.class);
    }

    @Override
    public RangeToken deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
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
