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

import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.palantir.atlasdb.api.RangeToken;

public class RangeTokenSerializer extends StdSerializer<RangeToken> {
    private static final long serialVersionUID = 1L;

    @Inject
    public RangeTokenSerializer() {
        super(RangeToken.class);
    }

    @Override
    public void serialize(RangeToken value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        jgen.writeObjectField("data", value.getResults());
        if (value.hasMoreResults()) {
            jgen.writeObjectField("next", value.getNextRange());
        }
        jgen.writeEndObject();
    }
}
