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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.persist.api.ReusablePersister;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

/**
 * A {@link ReusablePersister} that uses an {@link ObjectMapper} to serialize and deserialize objects
 * of type {@code T}.
 */
public abstract class JacksonPersister<T> implements ReusablePersister<T> {

    private final Class<T> typeRef;
    private final ObjectMapper mapper;

    public JacksonPersister(Class<T> typeRef, ObjectMapper mapper) {
        this.typeRef = typeRef;
        this.mapper = mapper;
    }

    @Override
    public final T hydrateFromBytes(byte[] input) {
        try {
            if (input.length <= 8192 && mapper.getFactory().canUseCharArrays()) {
                // Optimize to avoid allocation of heap ByteBuffer via InputStreamReader.
                // Remove after upgrade to Jackson 2.16.
                // see: https://github.com/FasterXML/jackson-core/pull/1081
                // and https://github.com/FasterXML/jackson-benchmarks/pull/9
                return mapper.readValue(new StringReader(new String(input, StandardCharsets.UTF_8)), typeRef);
            }
            return mapper.readValue(new ByteArrayInputStream(input), typeRef);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final byte[] persistToBytes(T value) {
        try {
            return mapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final Class<T> getPersistingClassType() {
        return typeRef;
    }
}
