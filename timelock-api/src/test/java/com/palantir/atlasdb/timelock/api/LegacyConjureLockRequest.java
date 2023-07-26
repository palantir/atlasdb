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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.palantir.conjure.java.lib.internal.ConjureCollections;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.Unsafe;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.processing.Generated;

// Copy of Conjure-generated file and prefixed with "Legcacy" to avoid conflicts
@Unsafe
@JsonDeserialize(builder = LegacyConjureLockRequest.Builder.class)
@Generated("com.palantir.conjure.java.types.BeanGenerator")
public final class LegacyConjureLockRequest {
    private final UUID requestId;

    private final Set<ConjureLockDescriptor> lockDescriptors;

    private final int acquireTimeoutMs;

    private final Optional<String> clientDescription;

    private int memoizedHashCode;

    private LegacyConjureLockRequest(
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
        return this == other
                || (other instanceof LegacyConjureLockRequest && equalTo((LegacyConjureLockRequest) other));
    }

    private boolean equalTo(LegacyConjureLockRequest other) {
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
        return "OldConjureLockRequest{requestId: " + requestId + ", lockDescriptors: " + lockDescriptors
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

    @Generated("com.palantir.conjure.java.types.BeanBuilderGenerator")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Builder {
        boolean _buildInvoked;

        private @Safe UUID requestId;

        private Set<ConjureLockDescriptor> lockDescriptors = new LinkedHashSet<>();

        private @Safe int acquireTimeoutMs;

        private Optional<@Unsafe String> clientDescription = Optional.empty();

        private boolean _acquireTimeoutMsInitialized = false;

        private Builder() {}

        public Builder from(LegacyConjureLockRequest other) {
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
            this.clientDescription = Preconditions.checkNotNull(clientDescription, "clientDescription cannot be null");
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

        public LegacyConjureLockRequest build() {
            checkNotBuilt();
            this._buildInvoked = true;
            validatePrimitiveFieldsHaveBeenInitialized();
            return new LegacyConjureLockRequest(requestId, lockDescriptors, acquireTimeoutMs, clientDescription);
        }

        private void checkNotBuilt() {
            Preconditions.checkState(!_buildInvoked, "Build has already been called");
        }
    }
}
