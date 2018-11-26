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

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.remoting3.ext.jackson.ObjectMappers;

public final class InternalSchemaMetadataPayloadCodec {
    private static int LATEST_VERSION = 1;

    private static Map<Integer, Function<byte[], InternalSchemaMetadata>> SUPPORTED_DECODERS =
            ImmutableMap.of(LATEST_VERSION, InternalSchemaMetadataPayloadCodec::decodeViaJson);
    private static ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    private InternalSchemaMetadataPayloadCodec() {
        // utility
    }

    static InternalSchemaMetadata decode(VersionedInternalSchemaMetadata versionedInternalSchemaMetadata) {
        Function<byte[], InternalSchemaMetadata> targetDeserializer
                = SUPPORTED_DECODERS.get(versionedInternalSchemaMetadata.version());
        if (targetDeserializer == null) {
            throw new SafeIllegalStateException("Could not decode persisted internal schema metadata -"
                    + " unrecognized version. This may occur transiently during upgrades, but if it persists please"
                    + " contact support.",
                    SafeArg.of("internalSchemaMetadata", versionedInternalSchemaMetadata),
                    SafeArg.of("knownVersions", SUPPORTED_DECODERS.keySet()));
        }
        return targetDeserializer.apply(versionedInternalSchemaMetadata.payload());
    }

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
