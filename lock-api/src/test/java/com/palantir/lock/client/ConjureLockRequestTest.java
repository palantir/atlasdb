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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import com.palantir.conjure.java.lib.internal.ConjureCollections;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.junit.Test;

public class ConjureLockRequestTest {
    private static final String BASE = "src/test/resources/conjure-lock-request-wire-format/";
    private static final String OLD_CONJURE_LOCK_REQUEST_FILE = "old";
    private static final String WITH_METADATA_FILE = "with-metadata";
    private static final boolean REWRITE_JSON_BLOBS = false;

    private static final ConjureLockDescriptor LOCK_1 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("abc")));
    private static final ConjureLockDescriptor LOCK_2 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("def")));
    private static final ConjureLockDescriptor LOCK_3 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("ghi")));
    private static final ConjureLockDescriptor LOCK_4 = ConjureLockDescriptor.of(Bytes.from(PtBytes.toBytes("jkl")));
    private static final UUID REQUEST_ID = new UUID(1337, 42);
    private static final int ACQUIRE_TIMEOUT_MS = 100;
    private static final String CLIENT_DESCRIPTION = "client: test, thread: test";
    private static final OldConjureLockRequest OLD_CONJURE_LOCK_REQUEST = OldConjureLockRequest.builder()
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
        assertSerializedEquals(OLD_CONJURE_LOCK_REQUEST, OLD_CONJURE_LOCK_REQUEST_FILE);
        assertDeserializedEquals(OLD_CONJURE_LOCK_REQUEST_FILE, OLD_CONJURE_LOCK_REQUEST, OldConjureLockRequest.class);
    }

    // Requires excludeEmptyOptionals = true
    @Test
    public void absentMetadataSerializesDeserializes() {
        assertSerializedEquals(DEFAULT_CONJURE_LOCK_REQUEST, OLD_CONJURE_LOCK_REQUEST_FILE);
        assertDeserializedEquals(OLD_CONJURE_LOCK_REQUEST_FILE, DEFAULT_CONJURE_LOCK_REQUEST, ConjureLockRequest.class);
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
        assertDeserializedEquals(OLD_CONJURE_LOCK_REQUEST_FILE, DEFAULT_CONJURE_LOCK_REQUEST, ConjureLockRequest.class);
    }

    // Possible because we do not use strict objects
    @Test
    public void oldServerCanHandlePresentMetadata() {
        assertDeserializedEquals(WITH_METADATA_FILE, OLD_CONJURE_LOCK_REQUEST, OldConjureLockRequest.class);
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

    // Copy of Conjure-generated file and prefixed with "Old"
    @Unsafe
    @JsonDeserialize(builder = OldConjureLockRequest.Builder.class)
    public static final class OldConjureLockRequest {
        private final UUID requestId;

        private final Set<ConjureLockDescriptor> lockDescriptors;

        private final int acquireTimeoutMs;

        private final Optional<String> clientDescription;

        private int memoizedHashCode;

        private OldConjureLockRequest(
                UUID requestId,
                Set<ConjureLockDescriptor> lockDescriptors,
                int acquireTimeoutMs,
                Optional<String> clientDescription) {
            validateFields(requestId, lockDescriptors, clientDescription);
            this.requestId = requestId;
            this.lockDescriptors = Collections.unmodifiableSet(lockDescriptors);
            this.acquireTimeoutMs = acquireTimeoutMs;
            this.clientDescription = clientDescription;
        }

        @JsonProperty("requestId")
        @Safe
        public UUID getRequestId() {
            return this.requestId;
        }

        @JsonProperty("lockDescriptors")
        public Set<ConjureLockDescriptor> getLockDescriptors() {
            return this.lockDescriptors;
        }

        @JsonProperty("acquireTimeoutMs")
        @Safe
        public int getAcquireTimeoutMs() {
            return this.acquireTimeoutMs;
        }

        @JsonProperty("clientDescription")
        @Unsafe
        public Optional<String> getClientDescription() {
            return this.clientDescription;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            return this == other || (other instanceof OldConjureLockRequest && equalTo((OldConjureLockRequest) other));
        }

        private boolean equalTo(OldConjureLockRequest other) {
            if (this.memoizedHashCode != 0
                    && other.memoizedHashCode != 0
                    && this.memoizedHashCode != other.memoizedHashCode) {
                return false;
            }
            return this.requestId.equals(other.requestId)
                    && this.lockDescriptors.equals(other.lockDescriptors)
                    && this.acquireTimeoutMs == other.acquireTimeoutMs
                    && this.clientDescription.equals(other.clientDescription);
        }

        @Override
        public int hashCode() {
            int result = memoizedHashCode;
            if (result == 0) {
                int hash = 1;
                hash = 31 * hash + this.requestId.hashCode();
                hash = 31 * hash + this.lockDescriptors.hashCode();
                hash = 31 * hash + this.acquireTimeoutMs;
                hash = 31 * hash + this.clientDescription.hashCode();
                result = hash;
                memoizedHashCode = result;
            }
            return result;
        }

        @Override
        @Unsafe
        public String toString() {
            return "ConjureLockRequest{requestId: " + requestId + ", lockDescriptors: " + lockDescriptors
                    + ", acquireTimeoutMs: " + acquireTimeoutMs + ", clientDescription: " + clientDescription + '}';
        }

        private static void validateFields(
                UUID requestId, Set<ConjureLockDescriptor> lockDescriptors, Optional<String> clientDescription) {
            List<String> missingFields = null;
            missingFields = addFieldIfMissing(missingFields, requestId, "requestId");
            missingFields = addFieldIfMissing(missingFields, lockDescriptors, "lockDescriptors");
            missingFields = addFieldIfMissing(missingFields, clientDescription, "clientDescription");
            if (missingFields != null) {
                throw new SafeIllegalArgumentException(
                        "Some required fields have not been set", SafeArg.of("missingFields", missingFields));
            }
        }

        private static List<String> addFieldIfMissing(List<String> prev, Object fieldValue, String fieldName) {
            List<String> missingFields = prev;
            if (fieldValue == null) {
                if (missingFields == null) {
                    missingFields = new ArrayList<>(3);
                }
                missingFields.add(fieldName);
            }
            return missingFields;
        }

        public static Builder builder() {
            return new Builder();
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static final class Builder {
            boolean _buildInvoked;

            private @Safe UUID requestId;

            private Set<ConjureLockDescriptor> lockDescriptors = new LinkedHashSet<>();

            private @Safe int acquireTimeoutMs;

            private Optional<@Unsafe String> clientDescription = Optional.empty();

            private boolean _acquireTimeoutMsInitialized = false;

            private Builder() {}

            public Builder from(OldConjureLockRequest other) {
                checkNotBuilt();
                requestId(other.getRequestId());
                lockDescriptors(other.getLockDescriptors());
                acquireTimeoutMs(other.getAcquireTimeoutMs());
                clientDescription(other.getClientDescription());
                return this;
            }

            @JsonSetter("requestId")
            public Builder requestId(@Nonnull @Safe UUID requestId) {
                checkNotBuilt();
                this.requestId = Preconditions.checkNotNull(requestId, "requestId cannot be null");
                return this;
            }

            @JsonSetter(value = "lockDescriptors", nulls = Nulls.SKIP)
            public Builder lockDescriptors(@Nonnull Iterable<ConjureLockDescriptor> lockDescriptors) {
                checkNotBuilt();
                this.lockDescriptors = ConjureCollections.newLinkedHashSet(
                        Preconditions.checkNotNull(lockDescriptors, "lockDescriptors cannot be null"));
                return this;
            }

            public Builder addAllLockDescriptors(@Nonnull Iterable<ConjureLockDescriptor> lockDescriptors) {
                checkNotBuilt();
                ConjureCollections.addAll(
                        this.lockDescriptors,
                        Preconditions.checkNotNull(lockDescriptors, "lockDescriptors cannot be null"));
                return this;
            }

            public Builder lockDescriptors(ConjureLockDescriptor lockDescriptors) {
                checkNotBuilt();
                this.lockDescriptors.add(lockDescriptors);
                return this;
            }

            @JsonSetter("acquireTimeoutMs")
            public Builder acquireTimeoutMs(@Safe int acquireTimeoutMs) {
                checkNotBuilt();
                this.acquireTimeoutMs = acquireTimeoutMs;
                this._acquireTimeoutMsInitialized = true;
                return this;
            }

            @JsonSetter(value = "clientDescription", nulls = Nulls.SKIP)
            public Builder clientDescription(@Nonnull Optional<@Unsafe String> clientDescription) {
                checkNotBuilt();
                this.clientDescription =
                        Preconditions.checkNotNull(clientDescription, "clientDescription cannot be null");
                return this;
            }

            public Builder clientDescription(@Nonnull @Unsafe String clientDescription) {
                checkNotBuilt();
                this.clientDescription =
                        Optional.of(Preconditions.checkNotNull(clientDescription, "clientDescription cannot be null"));
                return this;
            }

            private void validatePrimitiveFieldsHaveBeenInitialized() {
                List<String> missingFields = null;
                missingFields = addFieldIfMissing(missingFields, _acquireTimeoutMsInitialized, "acquireTimeoutMs");
                if (missingFields != null) {
                    throw new SafeIllegalArgumentException(
                            "Some required fields have not been set", SafeArg.of("missingFields", missingFields));
                }
            }

            private static List<String> addFieldIfMissing(List<String> prev, boolean initialized, String fieldName) {
                List<String> missingFields = prev;
                if (!initialized) {
                    if (missingFields == null) {
                        missingFields = new ArrayList<>(1);
                    }
                    missingFields.add(fieldName);
                }
                return missingFields;
            }

            public OldConjureLockRequest build() {
                checkNotBuilt();
                this._buildInvoked = true;
                validatePrimitiveFieldsHaveBeenInitialized();
                return new OldConjureLockRequest(requestId, lockDescriptors, acquireTimeoutMs, clientDescription);
            }

            private void checkNotBuilt() {
                Preconditions.checkState(!_buildInvoked, "Build has already been called");
            }
        }
    }
}
