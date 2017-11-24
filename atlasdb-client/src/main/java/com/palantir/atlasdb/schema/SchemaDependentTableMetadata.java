/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import org.immutables.value.Value;

import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence.CleanupRequirement;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

@Value.Immutable
public abstract class SchemaDependentTableMetadata implements Persistable {
    public static final Hydrator<SchemaDependentTableMetadata> HYDRATOR = input -> {
        try {
            SchemaMetadataPersistence.SchemaDependentTableMetadata message =
                    SchemaMetadataPersistence.SchemaDependentTableMetadata.parseFrom(input);
            return hydrateFromProto(message);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.throwUncheckedException(e);
        }
    };

    public abstract CleanupRequirement cleanupRequirement();

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    public SchemaMetadataPersistence.SchemaDependentTableMetadata persistToProto() {
        SchemaMetadataPersistence.SchemaDependentTableMetadata.Builder builder =
                SchemaMetadataPersistence.SchemaDependentTableMetadata.newBuilder();
        builder.setCleanupRequirement(cleanupRequirement());
        return builder.build();
    }

    public static SchemaDependentTableMetadata hydrateFromProto(
            SchemaMetadataPersistence.SchemaDependentTableMetadata message) {
        CleanupRequirement cleanupRequirement = CleanupRequirement.ARBITRARY_SYNC; // strictest default
        if (message.hasCleanupRequirement()) {
            cleanupRequirement = message.getCleanupRequirement();
        }
        return ImmutableSchemaDependentTableMetadata.builder()
                .cleanupRequirement(cleanupRequirement)
                .build();
    }
}
