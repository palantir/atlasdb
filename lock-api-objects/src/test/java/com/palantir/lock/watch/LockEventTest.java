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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.Test;

public class LockEventTest {
    private static final String BASE = "src/test/resources/lock-event-wire-format/";
    private static final boolean REWRITE_JSON_BLOBS = false;

    private static final LockDescriptor LOCK_1 = StringLockDescriptor.of("abc");
    private static final LockDescriptor LOCK_2 = StringLockDescriptor.of("test-lock");
    private static final OldLockEvent OLD_LOCK_EVENT = ImmutableOldLockEvent.builder()
            .sequence(10L)
            .lockDescriptors(ImmutableSet.of(LOCK_1, LOCK_2))
            .lockToken(LockToken.of(new UUID(1337, 42)))
            .build();
    private static final LockEvent BASELINE_LOCK_EVENT = ImmutableLockEvent.builder()
            .sequence(10L)
            .lockDescriptors(ImmutableSet.of(LOCK_1, LOCK_2))
            .lockToken(LockToken.of(new UUID(1337, 42)))
            .build();
    private static final LockRequestMetadata LOCK_REQUEST_METADATA = LockRequestMetadata.of(ImmutableMap.of(
            LOCK_1, ChangeMetadata.created(PtBytes.toBytes("new")), LOCK_2, ChangeMetadata.unchanged()));

    // These are the mappers used by Conjure (minus the pretty-printing)
    private static final ObjectMapper SERVER_MAPPER =
            ObjectMappers.newServerObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper CLIENT_MAPPER =
            ObjectMappers.newClientObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // This mapper is used to ensure that two JSONs are equal excluding indentation
    private static final ObjectMapper VERIFYING_MAPPER = new ObjectMapper();
    private static final Random RAND = new Random();

    @Test
    public void oldLockEventIsBaseline() {
        assertSerializedEquals(OLD_LOCK_EVENT, "baseline");
        assertDeserializedEquals("baseline", OLD_LOCK_EVENT, OldLockEvent.class);
    }

    @Test
    public void serializedFormatIsUnchangedForAbsentMetadata() {
        assertSerializedEquals(BASELINE_LOCK_EVENT, "baseline");
        assertDeserializedEquals("baseline", BASELINE_LOCK_EVENT, LockEvent.class);

        LockWatchEvent asLockWatchEvent = BASELINE_LOCK_EVENT;
        assertSerializedEquals(asLockWatchEvent, "baseline");
        assertDeserializedEquals("baseline", asLockWatchEvent, LockWatchEvent.class);
    }

    @Test
    public void serializesMetadataIfPresent() {
        LockEvent lockEventWithMetadata =
                ImmutableLockEvent.copyOf(BASELINE_LOCK_EVENT).withMetadata(LOCK_REQUEST_METADATA);
        assertSerializedEquals(lockEventWithMetadata, "baseline-with-metadata");
        assertDeserializedEquals("baseline-with-metadata", lockEventWithMetadata, LockEvent.class);

        LockWatchEvent asLockWatchEvent =
                ImmutableLockEvent.copyOf(BASELINE_LOCK_EVENT).withMetadata(LOCK_REQUEST_METADATA);
        assertSerializedEquals(asLockWatchEvent, "baseline-with-metadata");
        assertDeserializedEquals("baseline-with-metadata", asLockWatchEvent, LockWatchEvent.class);
    }

    @Test
    public void oldClientsIgnoreMetadataIfUsingConjure() {
        assertDeserializedEquals("baseline-with-metadata", OLD_LOCK_EVENT, OldLockEvent.class);
    }

    @Test
    public void canSerializeAndDeserializeLargeEventWithRandomMetadata() {
        List<LockDescriptor> lockDescriptors = Stream.generate(UUID::randomUUID)
                .map(UUID::toString)
                .map(StringLockDescriptor::of)
                .limit(10000)
                .collect(Collectors.toList());
        Map<LockDescriptor, ChangeMetadata> lockDescriptorToChangeMetadata = KeyedStream.of(lockDescriptors.stream())
                .filter(lockDescriptor -> RAND.nextBoolean())
                .map(lock -> createRandomChangeMetadata())
                .collectToMap();
        Collections.shuffle(lockDescriptors, RAND);
        LockEvent largeLockEvent = ImmutableLockEvent.builder()
                .sequence(10L)
                .lockDescriptors(new LinkedHashSet<>(lockDescriptors))
                .lockToken(LockToken.of(new UUID(1, 2)))
                .metadata(LockRequestMetadata.of(lockDescriptorToChangeMetadata))
                .build();

        assertThat(deserialize(serialize(largeLockEvent), LockEvent.class)).isEqualTo(largeLockEvent);
        LockWatchEvent asLockWatchEvent = largeLockEvent;
        assertThat(deserialize(serialize(asLockWatchEvent), LockWatchEvent.class))
                .isEqualTo(asLockWatchEvent);

        OldLockEvent largeLockEventWithoutMetadata = ImmutableOldLockEvent.builder()
                .sequence(largeLockEvent.sequence())
                .lockDescriptors(largeLockEvent.lockDescriptors())
                .lockToken(largeLockEvent.lockToken())
                .build();
        assertThat(deserialize(serialize(largeLockEvent), OldLockEvent.class)).isEqualTo(largeLockEventWithoutMetadata);
    }

    @Test
    public void shufflingLockDescriptorsOnTheWireIsDetectedByClient() {
        assertThatThrownBy(() ->
                        deserialize(Files.readString(getJsonPath("baseline-with-shuffled-locks")), LockEvent.class))
                .rootCause()
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageStartingWith("Key list integrity check failed");
    }

    private static <T> void assertSerializedEquals(T object, String jsonFileName) {
        try {
            Path path = getJsonPath(jsonFileName);
            if (REWRITE_JSON_BLOBS) {
                SERVER_MAPPER.writeValue(path.toFile(), object);
            }
            String serialized = serialize(object);
            assertThat(VERIFYING_MAPPER.readTree(serialized))
                    .as("Serialization yields semantically identical JSON representation")
                    .isEqualTo(VERIFYING_MAPPER.readTree(Files.readString(path)));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static <T> void assertDeserializedEquals(String jsonFileName, T object, Class<T> clazz) {
        try {
            assertThat(deserialize(Files.readString(getJsonPath(jsonFileName)), clazz))
                    .as("Deserialization yields identical object")
                    .isEqualTo(object);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static <T> String serialize(T object) {
        try {
            return SERVER_MAPPER.writeValueAsString(object);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static <T> T deserialize(String json, Class<T> clazz) {
        try {
            return CLIENT_MAPPER.readValue(json, clazz);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
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

    private static Path getJsonPath(String jsonFileName) {
        return Paths.get(BASE + jsonFileName + ".json");
    }

    private static ChangeMetadata createRandomChangeMetadata() {
        switch (RAND.nextInt(4)) {
            case 0:
                return ChangeMetadata.unchanged();
            case 1:
                return ChangeMetadata.updated(
                        PtBytes.toBytes(UUID.randomUUID().toString()),
                        PtBytes.toBytes(UUID.randomUUID().toString()));
            case 2:
                return ChangeMetadata.created(PtBytes.toBytes(UUID.randomUUID().toString()));
            case 3:
                return ChangeMetadata.deleted(PtBytes.toBytes(UUID.randomUUID().toString()));
            default:
                throw new IllegalStateException();
        }
    }
}
