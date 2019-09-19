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

package com.palantir.atlasdb.internalschema.persistence;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A serialized form of {@link InternalSchemaMetadata} that includes both a
 * version of the wire-format as well as a payload.
 *
 * Note that additive changes may be made to {@link InternalSchemaMetadata}
 * without requiring an increase in the version number; increases are only needed when wire-compatibility would be
 * broken, or it is not possible to assign a sensible default value for events occurring in the past.
 *
 * Note: Please be careful when using this object's equals() method, as that compares the exact bytes written
 * in the payload, and thus might consider semantically equivalent payloads to be different.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableVersionedInternalSchemaMetadata.class)
@JsonDeserialize(as = ImmutableVersionedInternalSchemaMetadata.class)
public interface VersionedInternalSchemaMetadata {
    int version();
    byte[] payload();

    /**
     * Returns the underlying {@link InternalSchemaMetadata} for this {@link VersionedInternalSchemaMetadata},
     * provided we are able to deserialize it. Otherwise returns empty.
     */
    @Value.Lazy
    @JsonIgnore
    default Optional<InternalSchemaMetadata> underlyingData() {
        return InternalSchemaMetadataPayloadCodec.tryDecode(this);
    }

    /**
     * Returns true iff this {@link VersionedInternalSchemaMetadata} object and the other object are knowably
     * semantically equivalent - that is, they have the same version, which this product version is able to understand,
     * and the {@link InternalSchemaMetadata} that underlies the payloads are equal.
     */
    @Value.Lazy
    @JsonIgnore
    default boolean knowablySemanticallyEquivalent(VersionedInternalSchemaMetadata other) {
        if (other == null || version() != other.version()) {
            return false;
        }
        return underlyingData().isPresent()
                && other.underlyingData().isPresent()
                && underlyingData().get().equals(other.underlyingData().get());
    }
}
