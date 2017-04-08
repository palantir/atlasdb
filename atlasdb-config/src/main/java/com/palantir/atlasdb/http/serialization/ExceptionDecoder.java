/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.http.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.http.AtlasDbHttpClients;

public class ExceptionDecoder {
    private static final Logger log = LoggerFactory.getLogger(ExceptionDecoder.class);

    private static final String CLASS_ANNOTATION = "@class";

    private final ObjectMapper mapper;

    private ExceptionDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static ExceptionDecoder create() {
        return new ExceptionDecoder(AtlasDbHttpClients.mapper);
    }

    public Optional<Exception> decodeCausingException(InputStream inputStream) {
        return readJsonNode(inputStream).flatMap(this::decodeException);
    }

    private Optional<JsonNode> readJsonNode(InputStream inputStream) {
        try {
            return Optional.of(mapper.readTree(inputStream));
        } catch (IOException e) {
            log.debug("Attempted to decode a JSON node from an exception, but failed", e);
            return Optional.empty();
        }
    }

    private Optional<Exception> decodeException(JsonNode jsonNode) {
        try {
            Class<? extends Exception> exceptionClass = readExceptionClass(jsonNode);
            return Optional.of(mapper.treeToValue(jsonNode, exceptionClass));
        } catch (JsonProcessingException e) {
            log.warn("Could not deserialize a JSON node passed in to an exception: {}", jsonNode, e);
            return Optional.empty();
        }
    }

    private Class<? extends Exception> readExceptionClass(JsonNode jsonNode) {
        if (jsonNode.has(CLASS_ANNOTATION)) {
            return Exception.class;
        }

        String className = jsonNode.get(CLASS_ANNOTATION).textValue();
        if (className == null) {
            // possible, if we have a malformed case and the exception class is an object
            return Exception.class;
        }
        try {
            return readExceptionClass(className);
        } catch (ClassNotFoundException e) {
            log.warn("The server threw an exception of class {} which the client doesn't know about",
                    jsonNode.get(CLASS_ANNOTATION).textValue(),
                    e);
            return Exception.class;
        }
    }

    private Class<? extends Exception> readExceptionClass(String className) throws ClassNotFoundException {
        return Class.forName(className).asSubclass(Exception.class);
    }
}
