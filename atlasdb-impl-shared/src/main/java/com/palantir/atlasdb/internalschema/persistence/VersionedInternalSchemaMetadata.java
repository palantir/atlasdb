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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.remoting3.ext.jackson.ObjectMappers;

/**
 * A serialized form of {@link com.palantir.atlasdb.internalschema.InternalSchemaMetadata} that includes both a
 * version of the wire-format as well as a payload.
 *
 * Note that additive changes may be made to {@link com.palantir.atlasdb.internalschema.InternalSchemaMetadata}
 * without requiring an increase in the version number; increases are only needed when wire-compatibility would be
 * broken, or it is not possible to assign a sensible default value for events occurring in the past.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableVersionedInternalSchemaMetadata.class)
@JsonDeserialize(as = ImmutableVersionedInternalSchemaMetadata.class)
public interface VersionedInternalSchemaMetadata {
    int version();
    byte[] payload();

    @Value.Default
    default String stringyPayload() {
        try {
            return ObjectMappers.newServerObjectMapper().readValue(payload(), InternalSchemaMetadata.class).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
