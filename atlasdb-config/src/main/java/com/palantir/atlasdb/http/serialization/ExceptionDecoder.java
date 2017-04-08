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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.common.remoting.AtlasDbRemotingConstants;

public class ExceptionDecoder {
    private static final Logger log = LoggerFactory.getLogger(ExceptionDecoder.class);

    public static final String CLASS_ANNOTATION = AtlasDbRemotingConstants.CLASS_FIELD_NAME;

    private final ObjectMapper mapper;

    private ExceptionDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static ExceptionDecoder create() {
        // Need to disable the feature to handle exceptions of unknown type, which may have arbitrary fields.
        return new ExceptionDecoder(new ObjectMapper()
                .registerModule(new GuavaModule())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
    }

    public Optional<Exception> decodeCausingException(InputStream inputStream) {
        return readJsonNode(inputStream).flatMap(this::decodeException);
    }

    @VisibleForTesting
    Optional<JsonNode> readJsonNode(InputStream inputStream) {
        try {
            return Optional.of(mapper.readTree(inputStream));
        } catch (IOException e) {
            log.debug("Attempted to decode a JSON node from an exception, but failed", e);
            return Optional.empty();
        }
    }

    private Optional<Exception> decodeException(JsonNode jsonNode) {
        try {
            Class<? extends Exception> exceptionClass = parseExceptionClass(jsonNode);
            return Optional.of(mapper.treeToValue(jsonNode, exceptionClass));
        } catch (JsonProcessingException e) {
            log.warn("Could not deserialize a JSON node passed in to an exception: {}", jsonNode, e);
            return Optional.empty();
        }
    }

    @VisibleForTesting
    static Class<? extends Exception> parseExceptionClass(JsonNode jsonNode) {
        if (!jsonNode.has(CLASS_ANNOTATION)) {
            return Exception.class;
        }

        String className = jsonNode.get(CLASS_ANNOTATION).textValue();
        Preconditions.checkArgument(className != null, "The class name must be a string, but found %s",
                jsonNode.get(CLASS_ANNOTATION).asText());
        try {
            return parseExceptionClass(className);
        } catch (ClassNotFoundException e) {
            log.warn("The server threw an exception of class {} which the client doesn't know about;"
                            + " deserializing as a generic Exception.",
                    jsonNode.get(CLASS_ANNOTATION).textValue(),
                    e);

            // We need to do this, otherwise the deserialization (line 65) will break
            ((ObjectNode) jsonNode).remove(CLASS_ANNOTATION);
            return Exception.class;
        }
    }

    private static Class<? extends Exception> parseExceptionClass(String className) throws ClassNotFoundException {
        return Class.forName(className).asSubclass(Exception.class);
    }
}
