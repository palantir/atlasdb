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

package com.palantir.atlasdb.timelock.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.junit.Test;

public class ConjureLockRequestTest {
    private static final String BASE = "src/test/resources/conjure-lock-request-wire-format/";
    private static final String LEGACY_CONJURE_LOCK_REQUEST_FILE = "old";
    private static final String WITH_METADATA_FILE = "with-metadata";
    private static final boolean REWRITE_JSON_BLOBS = false;

    private static final ConjureLockDescriptor LOCK_1 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("abc")));
    private static final ConjureLockDescriptor LOCK_2 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("def")));
    private static final ConjureLockDescriptor LOCK_3 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("ghi")));
    private static final ConjureLockDescriptor LOCK_4 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("jkl")));
    private static final UUID REQUEST_ID = new UUID(1337, 42);
    private static final int ACQUIRE_TIMEOUT_MS = 100;
    private static final String CLIENT_DESCRIPTION = "client: test, thread: test";
    private static final LegacyConjureLockRequest LEGACY_CONJURE_LOCK_REQUEST = LegacyConjureLockRequest.builder()
            .requestId(REQUEST_ID)
            .lockDescriptors(ImmutableSet.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4))
            .acquireTimeoutMs(ACQUIRE_TIMEOUT_MS)
            .clientDescription(CLIENT_DESCRIPTION)
            .build();
    private static final ConjureLockRequest DEFAULT_CONJURE_LOCK_REQUEST = ConjureLockRequest.builder()
            .requestId(REQUEST_ID)
            .lockDescriptors(ImmutableList.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4))
            .acquireTimeoutMs(ACQUIRE_TIMEOUT_MS)
            .clientDescription(CLIENT_DESCRIPTION)
            .build();

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

    // AtlasDB (Client) serializes and TimeLock (Server) deserializes ConjureLockRequest objects.
    // These are the respective mappers used internally by Conjure.
    private static final ObjectMapper SERIALIZATION_MAPPER =
            ObjectMappers.newClientObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper DESERIALIZATION_MAPPER =
            ObjectMappers.newServerObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    // This mapper is used to ensure that two JSONs are equal excluding indentation
    private static final ObjectMapper VERIFYING_MAPPER = new ObjectMapper();

    @Test
    public void oldConjureLockRequestSerializesDeserializes() {
        assertSerializedEquals(LEGACY_CONJURE_LOCK_REQUEST, LEGACY_CONJURE_LOCK_REQUEST_FILE);
        assertDeserializedEquals(
                LEGACY_CONJURE_LOCK_REQUEST_FILE, LEGACY_CONJURE_LOCK_REQUEST, LegacyConjureLockRequest.class);
    }

    @Test
    public void absentMetadataSerializesDeserializes() {
        // Because we use the Conjure configuration "excludeEmptyOptionals = true", we omit fields with empty optionals
        // during serialization instead of setting them to null.
        assertSerializedEquals(DEFAULT_CONJURE_LOCK_REQUEST, LEGACY_CONJURE_LOCK_REQUEST_FILE);
        assertDeserializedEquals(
                LEGACY_CONJURE_LOCK_REQUEST_FILE, DEFAULT_CONJURE_LOCK_REQUEST, ConjureLockRequest.class);
    }

    @Test
    public void presentMetadataSerializesDeserializes() {
        ConjureLockRequest requestWithMetadata = ConjureLockRequest.builder()
                .from(DEFAULT_CONJURE_LOCK_REQUEST)
                .metadata(CONJURE_LOCK_REQUEST_METADATA)
                .build();
        assertSerializedEquals(requestWithMetadata, WITH_METADATA_FILE);
        assertDeserializedEquals(WITH_METADATA_FILE, requestWithMetadata, ConjureLockRequest.class);
    }

    @Test
    public void newServerCanHandleMissingMetadata() {
        assertDeserializedEquals(
                LEGACY_CONJURE_LOCK_REQUEST_FILE, DEFAULT_CONJURE_LOCK_REQUEST, ConjureLockRequest.class);
    }

    @Test
    public void oldServerCanHandlePresentMetadata() {
        // Because we use the Conjure configuration "strictObjects = false", our Conjure objects actually ignore
        // unknown properties, even on the server side.
        assertDeserializedEquals(WITH_METADATA_FILE, LEGACY_CONJURE_LOCK_REQUEST, LegacyConjureLockRequest.class);
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> void assertDeserializedEquals(String jsonFileName, T object, Class<T> clazz) {
        try {
            assertThat(DESERIALIZATION_MAPPER.readValue(Files.readString(getJsonPath(jsonFileName)), clazz))
                    .as("Deserialization yields identical object")
                    .isEqualTo(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Path getJsonPath(String jsonFileName) {
        return Paths.get(BASE, jsonFileName + ".json");
    }
}
