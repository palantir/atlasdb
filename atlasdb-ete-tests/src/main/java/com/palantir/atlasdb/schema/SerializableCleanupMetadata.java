/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.schema;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.CleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;

/**
 * This class is somewhat hacky and should not be used outside of the atlasdb-ete-tests project.
 * It is used mainly to check correctness of {@link SchemaMetadata} persistence, without needing to make
 * SchemaMetadata and its dependencies JSON serializable and/or preserve its extensibility.
 */
@JsonSerialize(as = ImmutableSerializableCleanupMetadata.class)
@JsonDeserialize(as = ImmutableSerializableCleanupMetadata.class)
@Value.Immutable
public interface SerializableCleanupMetadata {
    String cleanupMetadataType();

    // The following fields should only be considered if cleanupMetadataType() returns "STREAM_STORE".
    Optional<Integer> numHashedRowComponents();
    Optional<String> streamIdType();

    CleanupMetadata.Visitor<SerializableCleanupMetadata> SERIALIZER
            = new CleanupMetadata.Visitor<SerializableCleanupMetadata>() {
        @Override
        public SerializableCleanupMetadata visit(NullCleanupMetadata cleanupMetadata) {
            return ImmutableSerializableCleanupMetadata.builder()
                    .cleanupMetadataType("NULL")
                    .build();
        }

        @Override
        public SerializableCleanupMetadata visit(StreamStoreCleanupMetadata cleanupMetadata) {
            return ImmutableSerializableCleanupMetadata.builder()
                    .cleanupMetadataType("STREAM_STORE")
                    .numHashedRowComponents(cleanupMetadata.numHashedRowComponents())
                    .streamIdType(cleanupMetadata.streamIdType().name())
                    .build();
        }

        @Override
        public SerializableCleanupMetadata visit(ArbitraryCleanupMetadata cleanupMetadata) {
            return ImmutableSerializableCleanupMetadata.builder()
                    .cleanupMetadataType("ARBITRARY")
                    .build();
        }
    };
}
