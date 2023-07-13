/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.api.ConjureChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureCreatedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureDeletedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptorListChecksum;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockRequestMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUnchangedChangeMetadata;
import com.palantir.atlasdb.timelock.api.ConjureUpdatedChangeMetadata;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.junit.Test;

public class ConjureLockRequestTest {
    private static final String BASE = "src/test/resources/conjure-lock-request-wire-format/";
    private static final boolean REWRITE_JSON_BLOBS = false;

    private static final ConjureLockDescriptor LOCK_1 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("abc")));
    private static final ConjureLockDescriptor LOCK_2 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("def")));
    private static final ConjureLockDescriptor LOCK_3 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("ghi")));
    private static final ConjureLockDescriptor LOCK_4 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("jkl")));
    private static final byte[] BYTES_OLD = PtBytes.toBytes("old");
    private static final byte[] BYTES_NEW = PtBytes.toBytes("new");
    private static final byte[] BYTES_DELETED = PtBytes.toBytes("deleted");
    private static final byte[] BYTES_CREATED = PtBytes.toBytes("created");
    // The checksum on this does not have to be correct since the integrity check is performed by the server and not
    // by Conjure itself
    private static final ConjureLockRequestMetadata CONJURE_LOCK_REQUEST_METADATA = ConjureLockRequestMetadata.of(
            ImmutableMap.of(
                    0,
                    ConjureChangeMetadata.unchanged(ConjureUnchangedChangeMetadata.of()),
                    1,
                    ConjureChangeMetadata.updated(
                            ConjureUpdatedChangeMetadata.of(Bytes.from(BYTES_OLD), Bytes.from(BYTES_NEW))),
                    2,
                    ConjureChangeMetadata.deleted(ConjureDeletedChangeMetadata.of(Bytes.from(BYTES_DELETED))),
                    3,
                    ConjureChangeMetadata.created(ConjureCreatedChangeMetadata.of(Bytes.from(BYTES_CREATED)))),
            ConjureLockDescriptorListChecksum.of(0, Bytes.from(PtBytes.toBytes("test-checksum-value"))));
    private static final ConjureLockRequest BASELINE_CONJURE_LOCK_REQUEST = ConjureLockRequest.builder()
            .requestId(new UUID(1337, 42))
            .lockDescriptors(ImmutableList.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4))
            .acquireTimeoutMs(100)
            .clientDescription("client: test, thread: test")
            .build();

    // AtlasDB (Client) serializes and TimeLock (Server) deserializes ConjureLockRequest objects.
    // These are the respective mappers used internally by Conjure.
    private static final ObjectMapper SERIALIZATION_MAPPER =
            ObjectMappers.newClientObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper DESERIALIZATION_MAPPER =
            ObjectMappers.newServerObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    // This mapper is used to ensure that two JSONs are equal excluding indentation
    private static final ObjectMapper VERIFYING_MAPPER = new ObjectMapper();

    @Test
    public void newServerCanHandleMissingMetadata() {
        assertDeserializedEquals("baseline", BASELINE_CONJURE_LOCK_REQUEST, ConjureLockRequest.class);
    }

    @Test
    public void serializesMetadataIfPresent() {
        ConjureLockRequest requestWithMetadata = ConjureLockRequest.builder()
                .from(BASELINE_CONJURE_LOCK_REQUEST)
                .metadata(CONJURE_LOCK_REQUEST_METADATA)
                .build();
        assertSerializedEquals(requestWithMetadata, "baseline-with-metadata");
        assertDeserializedEquals("baseline-with-metadata", requestWithMetadata, ConjureLockRequest.class);
    }

    private static <T> void assertSerializedEquals(T object, String jsonFileName) {
        try {
            Path path = getJsonPath(jsonFileName);
            if (REWRITE_JSON_BLOBS) {
                SERIALIZATION_MAPPER.writeValue(path.toFile(), object);
            }
            String serialized = SERIALIZATION_MAPPER.writeValueAsString(object);
            assertThat(VERIFYING_MAPPER.readTree(serialized))
                    .as("Serialization yields semantically identical JSON representation")
                    .isEqualTo(VERIFYING_MAPPER.readTree(Files.readString(path)));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static <T> void assertDeserializedEquals(String jsonFileName, T object, Class<T> clazz) {
        try {
            assertThat(DESERIALIZATION_MAPPER.readValue(Files.readString(getJsonPath(jsonFileName)), clazz))
                    .as("Deserialization yields identical object")
                    .isEqualTo(object);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static Path getJsonPath(String jsonFileName) {
        return Paths.get(BASE + jsonFileName + ".json");
    }
}
