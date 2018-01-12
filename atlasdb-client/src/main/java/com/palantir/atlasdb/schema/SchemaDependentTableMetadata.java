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
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.CleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

@Value.Immutable
public abstract class SchemaDependentTableMetadata implements Persistable {
    public static final Hydrator<SchemaDependentTableMetadata> BYTES_HYDRATOR = input -> {
        try {
            SchemaMetadataPersistence.SchemaDependentTableMetadata message =
                    SchemaMetadataPersistence.SchemaDependentTableMetadata.parseFrom(input);
            return hydrateFromProto(message);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.throwUncheckedException(e);
        }
    };

    public abstract CleanupMetadata cleanupMetadata();

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    public SchemaMetadataPersistence.SchemaDependentTableMetadata persistToProto() {
        SchemaMetadataPersistence.SchemaDependentTableMetadata.Builder builder =
                SchemaMetadataPersistence.SchemaDependentTableMetadata.newBuilder();
        cleanupMetadata().accept(new CleanupMetadataPersisterVisitor(builder));
        return builder.build();
    }

    public static SchemaDependentTableMetadata hydrateFromProto(
            SchemaMetadataPersistence.SchemaDependentTableMetadata message) {
        ImmutableSchemaDependentTableMetadata.Builder builder = ImmutableSchemaDependentTableMetadata.builder();
        builder.cleanupMetadata(parseCleanupMetadata(message));
        return builder.build();
    }

    private static CleanupMetadata parseCleanupMetadata(
            SchemaMetadataPersistence.SchemaDependentTableMetadata message) {
        switch (message.getCleanupMetadataCase()) {
            case ARBITRARYCLEANUPMETADATA:
                return ArbitraryCleanupMetadata.hydrateFromProto(message.getArbitraryCleanupMetadata());
            case NULLCLEANUPMETADATA:
                return NullCleanupMetadata.hydrateFromProto(message.getNullCleanupMetadata());
            case STREAMSTORECLEANUPMETADATA:
                return StreamStoreCleanupMetadata.hydrateFromProto(message.getStreamStoreCleanupMetadata());
            default:
                throw new IllegalStateException("Unexpected type of cleanup metadata found: "
                        + message.getCleanupMetadataCase());
        }
    }

    private static class CleanupMetadataPersisterVisitor implements CleanupMetadata.Visitor<Void> {
        private final SchemaMetadataPersistence.SchemaDependentTableMetadata.Builder builder;

        private CleanupMetadataPersisterVisitor(
                SchemaMetadataPersistence.SchemaDependentTableMetadata.Builder builder) {
            this.builder = builder;
        }

        @Override
        public Void visit(ArbitraryCleanupMetadata metadata) {
            builder.setArbitraryCleanupMetadata(metadata.persistToProto());
            return null;
        }

        @Override
        public Void visit(NullCleanupMetadata metadata) {
            builder.setNullCleanupMetadata(metadata.persistToProto());
            return null;
        }

        @Override
        public Void visit(StreamStoreCleanupMetadata metadata) {
            builder.setStreamStoreCleanupMetadata(metadata.persistToProto());
            return null;
        }
    }
}
