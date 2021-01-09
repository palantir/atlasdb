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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * The {@link InternalSchemaMetadataPayloadCodec} controls translation between the payload of a
 * {@link VersionedInternalSchemaMetadata} object, and an {@link InternalSchemaMetadata} object used by other
 * parts of AtlasDB.
 */
public final class InternalSchemaMetadataPayloadCodec {
    public static final int LATEST_VERSION = 1;

    /**
     * Mapping of supported schema versions to transforms that are able to deserialize serialized representations of
     * the metadata object associated with that version to a common {@link InternalSchemaMetadata} object.
     */
    private static final ImmutableMap<Integer, Function<byte[], InternalSchemaMetadata>> SUPPORTED_DECODERS =
            ImmutableMap.of(LATEST_VERSION, InternalSchemaMetadataPayloadCodec::decodeViaJson);

    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    private InternalSchemaMetadataPayloadCodec() {
        // utility
    }

    /**
     * Decodes a {@link VersionedInternalSchemaMetadata} object into a common {@link InternalSchemaMetadata} object,
     * provided this instance of the codec knows how to decode the relevant
     * {@link VersionedInternalSchemaMetadata#version()}. This method throws if the version is not recognised.
     *
     * @param versionedInternalSchemaMetadata internal schema metadata object to decode
     * @return common representation of schema metadata
     */
    static InternalSchemaMetadata decode(VersionedInternalSchemaMetadata versionedInternalSchemaMetadata) {
        return tryDecode(versionedInternalSchemaMetadata)
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Could not decode persisted internal schema metadata - unrecognized version. This may occur"
                                + " transiently during upgrades, but if it persists please contact support.",
                        SafeArg.of("internalSchemaMetadata", versionedInternalSchemaMetadata),
                        SafeArg.of("knownVersions", SUPPORTED_DECODERS.keySet())));
    }

    static Optional<InternalSchemaMetadata> tryDecode(VersionedInternalSchemaMetadata versionedInternalSchemaMetadata) {
        Function<byte[], InternalSchemaMetadata> targetDeserializer =
                SUPPORTED_DECODERS.get(versionedInternalSchemaMetadata.version());
        if (targetDeserializer == null) {
            return Optional.empty();
        }
        return Optional.of(targetDeserializer.apply(versionedInternalSchemaMetadata.payload()));
    }

    /**
     * Encodes an {@link InternalSchemaMetadata} object to a {@link VersionedInternalSchemaMetadata} object, always
     * using the {@link InternalSchemaMetadataPayloadCodec#LATEST_VERSION} of the schema.
     *
     * @param internalSchemaMetadata common representation of schema metadata to encode
     * @return encoded, versioned form of the schema metadata
     */
    static VersionedInternalSchemaMetadata encode(InternalSchemaMetadata internalSchemaMetadata) {
        try {
            return ImmutableVersionedInternalSchemaMetadata.builder()
                    .version(LATEST_VERSION)
                    .payload(OBJECT_MAPPER.writeValueAsBytes(internalSchemaMetadata))
                    .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static InternalSchemaMetadata decodeViaJson(byte[] byteArray) {
        try {
            return OBJECT_MAPPER.readValue(byteArray, InternalSchemaMetadata.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
