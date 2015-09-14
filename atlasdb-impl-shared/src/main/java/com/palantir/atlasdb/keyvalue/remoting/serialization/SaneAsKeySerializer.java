/**
 * Copyright 2015 Palantir Technologies
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