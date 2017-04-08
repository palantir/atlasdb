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
package com.palantir.common.remoting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.net.HostAndPort;

public class ServiceNotAvailableExceptionSerializationTest {
    private static final Exception GENERIC_EXCEPTION = new Exception("Something bad happened!");
    private static final ServiceNotAvailableException SERVICE_NOT_AVAILABLE_EXCEPTION =
            new ServiceNotAvailableException(
                    "The server is sad",
                    GENERIC_EXCEPTION,
                    HostAndPort.fromParts("palantir", 3142));

    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new GuavaModule());

    @Test
    public void canSerializeAndDeserializeCorrectly() throws IOException {
        String serialized = mapper.writeValueAsString(SERVICE_NOT_AVAILABLE_EXCEPTION);
        ServiceNotAvailableException deserialized = mapper.readValue(serialized, ServiceNotAvailableException.class);
        assertThat(deserialized.getMessage(), is(SERVICE_NOT_AVAILABLE_EXCEPTION.getMessage()));
        assertThat(deserialized.getCause().getMessage(), is(SERVICE_NOT_AVAILABLE_EXCEPTION.getCause().getMessage()));
        assertThat(deserialized.getServiceHint(), is(SERVICE_NOT_AVAILABLE_EXCEPTION.getServiceHint()));
    }

    @Test
    public void hasTypeInformationSerialized() throws IOException {
        String serialized = mapper.writeValueAsString(SERVICE_NOT_AVAILABLE_EXCEPTION);
        JsonNode jsonNode = mapper.readTree(serialized);

        assertThat(jsonNode.get(AtlasDbRemotingConstants.CLASS_FIELD_NAME).textValue(),
                is(SERVICE_NOT_AVAILABLE_EXCEPTION.getClass().getName()));
    }
}
