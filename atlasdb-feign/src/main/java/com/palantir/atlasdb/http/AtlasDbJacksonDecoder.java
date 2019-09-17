/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Copyright 2013 Netflix, Inc.
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
 *
 * Changes made:
 * - Rename to AtlasDbJacksonDecoder
 * - Checkstyle
 * - Use inputstreams and not stream readers - avoids creating a 16kB buffer on every response.
 * - Remove unused constructors
 */
package com.palantir.atlasdb.http;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

import feign.Response;
import feign.Util;
import feign.codec.Decoder;

class AtlasDbJacksonDecoder implements Decoder {
    private final ObjectMapper mapper;

    AtlasDbJacksonDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object decode(Response response, Type type) throws IOException {
      if (response.status() == 404) {
        return Util.emptyValueOf(type);
      }
      if (response.body() == null) {
        return null;
      }
        InputStream stream = response.body().asInputStream();
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream, 1);
        }
        try {
            // Read the first byte to see if we have any data
            stream.mark(1);
            if (stream.read() == -1) {
                return null; // Eagerly returning null avoids "No content to map due to end-of-input"
            }
            stream.reset();
            return mapper.readValue(stream, mapper.constructType(type));
        } catch (RuntimeJsonMappingException e) {
            if (e.getCause() != null && e.getCause() instanceof IOException) {
                throw IOException.class.cast(e.getCause());
            }
            throw e;
        }
    }
}
