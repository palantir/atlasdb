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
package com.palantir.atlasdb.persister;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.annotation.Reusable;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.common.base.Throwables;
import java.io.IOException;


@Reusable
public class JsonNodePersister implements Persister<JsonNode> {
    static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] persistToBytes(JsonNode jsonNode) {
        try {
            return mapper.writeValueAsBytes(jsonNode);
        } catch (JsonProcessingException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public JsonNode hydrateFromBytes(byte[] input) {
        try {
            return mapper.readTree(input);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public Class<JsonNode> getPersistingClassType() {
        return JsonNode.class;
    }
}
