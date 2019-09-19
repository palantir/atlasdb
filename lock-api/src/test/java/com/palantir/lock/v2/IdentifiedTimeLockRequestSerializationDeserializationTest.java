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
package com.palantir.lock.v2;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import org.junit.Test;

public class IdentifiedTimeLockRequestSerializationDeserializationTest {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();
    private static final UUID SAMPLE_UUID = UUID.fromString("ed4b106f-8718-4858-a402-d99415124cfe");
    private static final IdentifiedTimeLockRequest REQUEST = ImmutableIdentifiedTimeLockRequest.of(SAMPLE_UUID);

    private static final File IDENTIFIED_TIMELOCK_REQUEST_JSON = new File(
            IdentifiedTimeLockRequest.class.getResource("/identified-timelock-request.json").getPath());

    @Test
    public void canSerializeAndDeserializeRandomUuids() throws Exception {
        IdentifiedTimeLockRequest request = ImmutableIdentifiedTimeLockRequest.of(UUID.randomUUID());

        assertThat(OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(request), IdentifiedTimeLockRequest.class))
                .isEqualTo(request);
    }

    @Test
    public void canDeserializeIdentifiedTimeLockRequestFromJson() throws IOException {
        assertThat(OBJECT_MAPPER.readValue(IDENTIFIED_TIMELOCK_REQUEST_JSON, IdentifiedTimeLockRequest.class))
                .isEqualTo(REQUEST);
    }

    @Test
    public void canSerializeIdentifiedTimeLockRequestToJson() throws IOException {
        assertThat(OBJECT_MAPPER.writeValueAsString(REQUEST))
                .isEqualTo(new String(
                        Files.readAllBytes(IDENTIFIED_TIMELOCK_REQUEST_JSON.toPath()),
                        StandardCharsets.UTF_8));
    }
}
