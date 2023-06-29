/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockRequestMetadata;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.junit.Test;

public class IdentifiedLockRequestTest {
    private static final String BASE = "src/test/resources/identified-lock-request-wire-format/";
    private static final boolean REWRITE_JSON_BLOBS = false;
    private static final IdentifiedLockRequest BASELINE_REQUEST = ImmutableIdentifiedLockRequest.builder()
            .requestId(new UUID(1337, 42))
            .lockDescriptors(ImmutableSet.of(StringLockDescriptor.of("lock1"), StringLockDescriptor.of("lock2")))
            .acquireTimeoutMs(100)
            .clientDescription("client: test, thread: test")
            .build();
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(ImmutableMap.of(
            StringLockDescriptor.of("lock1"), ChangeMetadata.created("something".getBytes(StandardCharsets.UTF_8))));
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .registerModule(new Jdk8Module())
            .registerModule(new GuavaModule());

    @Test
    public void baselineRequestIsFullCompat() {
        assertSerializedEquals(BASELINE_REQUEST, "baseline");
        assertDeserializedEquals("baseline", BASELINE_REQUEST);
    }

    @Test
    public void baselineRequestWithMetadataIsBackCompat() {
        assertSerializedEquals(
                ImmutableIdentifiedLockRequest.copyOf(BASELINE_REQUEST).withMetadata(LOCK_REQUEST_METADATA),
                "baseline");
    }

    private static void assertSerializedEquals(IdentifiedLockRequest request, String jsonFileName) {
        try {
            Path path = getJsonPath(jsonFileName);
            if (REWRITE_JSON_BLOBS) {
                MAPPER.writeValue(path.toFile(), request);
            }
            String serialized = serialize(request);
            assertThat(MAPPER.readTree(serialized))
                    .as("Serialization yields identical JSON representation")
                    .isEqualTo(MAPPER.readTree(Files.readString(path)));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static void assertDeserializedEquals(String jsonFileName, IdentifiedLockRequest request) {
        assertThat(deserialize(jsonFileName)).isEqualTo(request);
    }

    private static String serialize(IdentifiedLockRequest request) {
        try {
            return MAPPER.writeValueAsString(request);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static IdentifiedLockRequest deserialize(String jsonFileName) {
        try {
            return MAPPER.readValue(Files.readString(getJsonPath(jsonFileName)), IdentifiedLockRequest.class);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static Path getJsonPath(String jsonFileName) {
        return Paths.get(BASE + jsonFileName + ".json");
    }
}
