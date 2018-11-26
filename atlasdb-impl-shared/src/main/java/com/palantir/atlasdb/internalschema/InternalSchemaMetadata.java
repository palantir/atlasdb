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

package com.palantir.atlasdb.internalschema;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * An {@link InternalSchemaMetadata} object controls how Atlas nodes carry out certain operations.
 *
 * This object may concurrently be read across nodes on multiple versions of AtlasDB, and thus it is important
 * to ensure backwards compatibility.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableInternalSchemaMetadata.class)
@JsonDeserialize(as = ImmutableInternalSchemaMetadata.class)
public interface InternalSchemaMetadata {
    TimestampPartitioningMap<Integer> timestampToTransactionsTableSchemaVersion();

    static ImmutableInternalSchemaMetadata.Builder builder() {
        return ImmutableInternalSchemaMetadata.builder();
    }
}
