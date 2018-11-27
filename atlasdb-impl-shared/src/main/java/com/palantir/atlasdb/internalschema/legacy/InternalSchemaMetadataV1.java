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

package com.palantir.atlasdb.internalschema.legacy;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;

/**
 * An {@link InternalSchemaMetadataV1} object controls how Atlas nodes carry out certain operations.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableInternalSchemaMetadataV1.class)
@JsonDeserialize(as = ImmutableInternalSchemaMetadataV1.class)
public interface InternalSchemaMetadataV1 {
    TimestampPartitioningMap<Integer> timestampToTransactionsTableSchemaVersion();

    static ImmutableInternalSchemaMetadataV1.Builder builder() {
        return ImmutableInternalSchemaMetadataV1.builder();
    }
}
