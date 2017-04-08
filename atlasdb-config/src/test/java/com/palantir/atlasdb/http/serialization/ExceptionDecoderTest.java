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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.InputStream;
import java.util.Optional;

import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.palantir.lock.remoting.BlockingTimeoutException;

public class ExceptionDecoderTest {
    private static final String EXCEPTION_MESSAGE_FIELD_NAME = "message";
    private static final String EXCEPTION_MESSAGE = "Something bad happened!";
    private static final Class<? extends Exception> KNOWN_EXCEPTION = BlockingTimeoutException.class;

    private static final InputStream MALFORMED_JSON_STREAM = new StringInputStream("{'1203");

    private static final JsonNode NO_CLASS_ANNOTATION_NODE = JsonNodeFactory.instance.objectNode()
            .put(EXCEPTION_MESSAGE_FIELD_NAME, EXCEPTION_MESSAGE);
    private static final JsonNode KNOWN_NON_EXCEPTION_CLASS_NODE = JsonNodeFactory.instance.objectNode()
            .put(ExceptionDecoder.CLASS_ANNOTATION, ExceptionDecoder.class.getName());
    private static final JsonNode UNKNOWN_EXCEPTION_CLASS_NODE = JsonNodeFactory.instance.objectNode()
            .put(ExceptionDecoder.CLASS_ANNOTATION, "foo.bar.SomeWeirdOtherException")
            .put(EXCEPTION_MESSAGE_FIELD_NAME, EXCEPTION_MESSAGE);
    private static final JsonNode KNOWN_EXCEPTION_CLASS_NODE = JsonNodeFactory.instance.objectNode()
            .put(ExceptionDecoder.CLASS_ANNOTATION, KNOWN_EXCEPTION.getName())
            .put(EXCEPTION_MESSAGE_FIELD_NAME, EXCEPTION_MESSAGE);
    private static final JsonNode MALFORMED_EXCEPTION_CLASS_NODE = JsonNodeFactory.instance.objectNode()
            .set(ExceptionDecoder.CLASS_ANNOTATION, JsonNodeFactory.instance.objectNode());

    private final ExceptionDecoder decoder = ExceptionDecoder.create();

    @Test
    public void readJsonNodeReturnsEmptyIfReadingMalformedJson() {
        assertThat(decoder.readJsonNode(MALFORMED_JSON_STREAM)).isNotPresent();
    }

    @Test
    public void readJsonNodeReturnsNonEmptyIfReadingValidJson() {
        InputStream inputStream = new StringInputStream("\"foo\"");
        assertThat(decoder.readJsonNode(inputStream)).isPresent();
    }

    @Test
    public void readExceptionClassReturnsGenericExceptionWithoutClassAnnotation() {
        assertThat(ExceptionDecoder.parseExceptionClass(NO_CLASS_ANNOTATION_NODE)).isEqualTo(Exception.class);
    }

    @Test
    public void readExceptionClassThrowsIfClassAnnotationIsKnownToBeNotAnException() {
        assertThatThrownBy(() -> ExceptionDecoder.parseExceptionClass(KNOWN_NON_EXCEPTION_CLASS_NODE))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    public void readExceptionClassReturnsGenericExceptionIfClassAnnotationIsNotKnown() {
        assertThat(ExceptionDecoder.parseExceptionClass(UNKNOWN_EXCEPTION_CLASS_NODE)).isEqualTo(Exception.class);
    }

    @Test
    public void readExceptionClassReturnsSpecificExceptionIfKnown() {
        assertThat(ExceptionDecoder.parseExceptionClass(KNOWN_EXCEPTION_CLASS_NODE))
                .isEqualTo(KNOWN_EXCEPTION);
    }

    @Test
    public void readExceptionClassThrowsIfExceptionClassIsNotATextField() {
        assertThatThrownBy(() -> ExceptionDecoder.parseExceptionClass(MALFORMED_EXCEPTION_CLASS_NODE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void decodeCausingExceptionReturnsEmptyIfReadingMalformedJson() {
        assertThat(decoder.decodeCausingException(MALFORMED_JSON_STREAM)).isNotPresent();
    }

    @Test
    public void decodeCausingExceptionWithoutClassAnnotationReturnsGenericException() {
        Optional<Exception> exceptionOptional =
                decoder.decodeCausingException(createInputStreamFor(NO_CLASS_ANNOTATION_NODE));

        assertTypeOfDecodedException(exceptionOptional, Exception.class);
    }

    @Test
    public void decodeCausingExceptionOfUnknownTypeReturnsGenericException() {
        Optional<Exception> exceptionOptional =
                decoder.decodeCausingException(createInputStreamFor(UNKNOWN_EXCEPTION_CLASS_NODE));

        assertTypeOfDecodedException(exceptionOptional, Exception.class);
    }

    @Test
    public void decodeCausingExceptionOfKnownTypeReturnsSpecificException() {
        Optional<Exception> exceptionOptional =
                decoder.decodeCausingException(createInputStreamFor(KNOWN_EXCEPTION_CLASS_NODE));

        assertTypeOfDecodedException(exceptionOptional, KNOWN_EXCEPTION);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType") // test assertion
    private static void assertTypeOfDecodedException(Optional<Exception> exceptionOptional, Class<?> clazz) {
        assertThat(exceptionOptional).isPresent();
        assertThat(exceptionOptional.get())
                .hasMessage(EXCEPTION_MESSAGE)
                .isExactlyInstanceOf(clazz);
    }

    private static InputStream createInputStreamFor(JsonNode jsonNode) {
        return new StringInputStream(jsonNode.toString());
    }
}
