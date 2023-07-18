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

package com.palantir.lock.watch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;
import org.junit.Test;

public class LockEventTest {
    private static final String BASE = "src/test/resources/lock-event-wire-format/";
    private static final String OLD_LOCK_EVENT_FILE = "old";
    private static final String WITH_METADATA_FILE = "with-metadata";
    private static final String WITH_EMPTY_METADATA_MAP_FILE = "with-empty-metadata-map";
    private static final String SPARSE_METADATA_FILE = "sparse-metadata";
    private static final String SHUFFLED_LOCKS_FILE = "shuffled-locks";
    private static final boolean REWRITE_JSON_BLOBS = false;

    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("abc");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("def");
    private static final LockDescriptor LOCK_3 = StringLockDescriptor.of("ghi");
    private static final LockDescriptor LOCK_4 = StringLockDescriptor.of("jkl");
    private static final long SEQUENCE = 10L;
    private static final LockToken LOCK_TOKEN = LockToken.of(new UUID(1337, 42));
    private static final Set<LockDescriptor> LOCK_SET = ImmutableSet.of(LOCK_1, LOCK_2, LOCK_3, LOCK_4);
    private static final OldLockEvent OLD_LOCK_EVENT = ImmutableOldLockEvent.builder()
            .sequence(SEQUENCE)
            .lockDescriptors(LOCK_SET)
            .lockToken(LOCK_TOKEN)
            .build();
    private static final LockEvent DEFAULT_LOCK_EVENT = ImmutableLockEvent.builder()
            .sequence(SEQUENCE)
            .lockDescriptors(LOCK_SET)
            .lockToken(LOCK_TOKEN)
            .build();
    private static final byte[] BYTES_OLD = PtBytes.toBytes("old");
    private static final byte[] BYTES_NEW = PtBytes.toBytes("new");
    private static final byte[] BYTES_DELETED = PtBytes.toBytes("deleted");
    private static final byte[] BYTES_CREATED = PtBytes.toBytes("created");
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(ImmutableMap.of(
            LOCK_1,
            ChangeMetadata.unchanged(),
            LOCK_2,
            ChangeMetadata.updated(BYTES_OLD, BYTES_NEW),
            LOCK_3,
            ChangeMetadata.deleted(BYTES_DELETED),
            LOCK_4,
            ChangeMetadata.created(BYTES_CREATED)));

    // TimeLock (Server) serializes and AtlasDB (Client) deserializes.
    // These are the respective mappers used internally by Conjure.
    private static final ObjectMapper SERIALIZATION_MAPPER =
            ObjectMappers.newServerObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper DESERIALIZATION_MAPPER =
            ObjectMappers.newClientObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    // This mapper is used to ensure that two JSONs are equal excluding indentation
    private static final ObjectMapper VERIFYING_MAPPER = new ObjectMapper();

    @Test
    public void oldLockEventSerializesDeserializes() {
        assertSerializedEquals(OLD_LOCK_EVENT, OLD_LOCK_EVENT_FILE);
        assertDeserializedEquals(OLD_LOCK_EVENT_FILE, OLD_LOCK_EVENT, OldLockEvent.class);
    }

    @Test
    public void absentMetadataSerializesDeserializes() {
        assertSerializedEquals(DEFAULT_LOCK_EVENT, OLD_LOCK_EVENT_FILE);
        assertDeserializedEquals(OLD_LOCK_EVENT_FILE, DEFAULT_LOCK_EVENT, LockEvent.class);
    }

    @Test
    public void presentMetadataSerializesDeserializes() {
        LockEvent lockEventWithMetadata =
                ImmutableLockEvent.copyOf(DEFAULT_LOCK_EVENT).withMetadata(LOCK_REQUEST_METADATA);
        assertSerializedEquals(lockEventWithMetadata, WITH_METADATA_FILE);
        assertDeserializedEquals(WITH_METADATA_FILE, lockEventWithMetadata, LockEvent.class);
    }

    @Test
    public void serializesDeserializesAsLockWatchEventWithMetadata() {
        LockWatchEvent lockWatchEvent =
                ImmutableLockEvent.copyOf(DEFAULT_LOCK_EVENT).withMetadata(LOCK_REQUEST_METADATA);
        assertSerializedEquals(lockWatchEvent, WITH_METADATA_FILE);
        assertDeserializedEquals(WITH_METADATA_FILE, lockWatchEvent, LockWatchEvent.class);
    }

    @Test
    public void emptyMetadataMapSerializesDeserializes() {
        LockEvent lockEventWithMetadata =
                ImmutableLockEvent.copyOf(DEFAULT_LOCK_EVENT).withMetadata(LockRequestMetadata.of(ImmutableMap.of()));
        assertSerializedEquals(lockEventWithMetadata, WITH_EMPTY_METADATA_MAP_FILE);
        assertDeserializedEquals(WITH_EMPTY_METADATA_MAP_FILE, lockEventWithMetadata, LockEvent.class);
    }

    // This behavior should be guaranteed by the Conjure spec
    @Test
    public void oldClientIgnoresMetadata() {
        assertDeserializedEquals(WITH_METADATA_FILE, OLD_LOCK_EVENT, OldLockEvent.class);
    }

    @Test
    public void sparseMetadataSerializesDeserializes() {
        List<LockDescriptor> lockDescriptors = IntStream.range(0, 10)
                .mapToObj(Integer::toString)
                .map(StringLockDescriptor::of)
                .collect(Collectors.toList());
        // Unique metadata on some locks, but not all
        Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata = ImmutableMap.of(
                lockDescriptors.get(0), ChangeMetadata.created(BYTES_CREATED),
                lockDescriptors.get(4), ChangeMetadata.deleted(BYTES_DELETED),
                lockDescriptors.get(9), ChangeMetadata.unchanged(),
                lockDescriptors.get(5), ChangeMetadata.updated(BYTES_OLD, BYTES_NEW),
                lockDescriptors.get(7), ChangeMetadata.unchanged());
        LockEvent lockEventWithSparseMetadata = ImmutableLockEvent.builder()
                .lockDescriptors(lockDescriptors)
                .sequence(SEQUENCE)
                .lockToken(LOCK_TOKEN)
                .metadata(LockRequestMetadata.of(lockDescriptorToChangeMetadata))
                .build();

        assertSerializedEquals(lockEventWithSparseMetadata, SPARSE_METADATA_FILE);
        assertDeserializedEquals(SPARSE_METADATA_FILE, lockEventWithSparseMetadata, LockEvent.class);
    }

    @Test
    public void shufflingLockDescriptorsOnTheWireIsDetectedByClient() {
        assertThatThrownBy(() ->
                        // The order of the lock descriptors in this JSON file is swapped
                        DESERIALIZATION_MAPPER.readValue(
                                Files.readString(getJsonPath(SHUFFLED_LOCKS_FILE)), LockEvent.class))
                .rootCause()
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageStartingWith("Key list integrity check failed");
    }

    @Test
    public void cannotDeserializeAsWireLockEvent() {
        assertThatThrownBy(() -> DESERIALIZATION_MAPPER.readValue(
                        Files.readString(getJsonPath(WITH_METADATA_FILE)), LockEvent.WireLockEvent.class))
                .isInstanceOf(InvalidDefinitionException.class);
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
        return Paths.get(BASE + jsonFileName + ".json");
    }

    // This is a copy of the LockEvent class before metadata was added
    @Value.Immutable
    @JsonSerialize(as = ImmutableOldLockEvent.class)
    @JsonDeserialize(as = ImmutableOldLockEvent.class)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonTypeName(LockEvent.TYPE)
    public interface OldLockEvent {
        long sequence();

        Set<LockDescriptor> lockDescriptors();

        LockToken lockToken();
    }
}
