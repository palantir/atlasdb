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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.remoting2.errors.SerializableError;
import java.io.File;
import java.io.IOException;
import javax.ws.rs.core.Response;
import org.junit.Test;

public class ExceptionMappersSerializationTest {
    private static final ObjectMapper MAPPER = ObjectMappers.newServerObjectMapper();

    private static final StackTraceElement[] TRACES = {new StackTraceElement("class", "method", null, 1)};
    private static final ServiceNotAvailableException SERVICE_NOT_AVAILABLE_EXCEPTION =
            new ServiceNotAvailableException("Service Unavailable");
    private static final int STATUS_CODE = 503;

    private static final String JSON_RESOURCE_PATH = "/fixtures/service-unavailable-exception.json";
    private static final File TEST_CONFIG_FILE = new File(ExceptionMappersSerializationTest.class
            .getResource(JSON_RESOURCE_PATH)
            .getPath());

    static {
        MAPPER.registerModule(new Jdk8Module());
        SERVICE_NOT_AVAILABLE_EXCEPTION.setStackTrace(TRACES);
    }

    @Test
    public void canSerializeExceptions() throws IOException {
        Response exceptionResponse = ExceptionMappers.encodeExceptionResponse(
                        SERVICE_NOT_AVAILABLE_EXCEPTION, STATUS_CODE)
                .build();
        Object exceptionEntity = exceptionResponse.getEntity();

        String serializedException = MAPPER.writeValueAsString(exceptionEntity);

        String expected = getNormalizedRepresentationOfExpectedJson();
        assertThat(serializedException).isEqualTo(expected);
    }

    private String getNormalizedRepresentationOfExpectedJson() throws IOException {
        // Reading/writing normalizes the formatting of the JSON itself.
        return MAPPER.writeValueAsString(MAPPER.readValue(TEST_CONFIG_FILE, SerializableError.class));
    }
}
