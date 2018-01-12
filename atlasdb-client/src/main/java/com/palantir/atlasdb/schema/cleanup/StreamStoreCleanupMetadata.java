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

package com.palantir.atlasdb.schema.cleanup;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence.StreamStoreCleanupV1Metadata;
import com.palantir.atlasdb.table.description.ValueType;

@Value.Immutable
public abstract class StreamStoreCleanupMetadata implements CleanupMetadata {
    private static final Logger log = LoggerFactory.getLogger(StreamStoreCleanupMetadata.class);

    public static final Hydrator<StreamStoreCleanupMetadata> BYTES_HYDRATOR = message -> {
        try {
            return hydrateFromProto(SchemaMetadataPersistence.StreamStoreCleanupMetadata.parseFrom(message));
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    };

    public static StreamStoreCleanupMetadata hydrateFromProto(
            SchemaMetadataPersistence.StreamStoreCleanupMetadata streamStoreCleanupMetadata) {
        ImmutableStreamStoreCleanupMetadata.Builder builder = ImmutableStreamStoreCleanupMetadata.builder();
        if (streamStoreCleanupMetadata.hasV1Metadata()) {
            StreamStoreCleanupV1Metadata v1Metadata = streamStoreCleanupMetadata.getV1Metadata();
            if (v1Metadata.hasNumHashedRowComponents()) {
                builder.numHashedRowComponents(v1Metadata.getNumHashedRowComponents());
            }
            if (v1Metadata.hasStreamIdType()) {
                builder.streamIdType(ValueType.hydrateFromProto(v1Metadata.getStreamIdType()));
            }
        } else {
            log.warn("Encountered stream store cleanup metadata without v1 metadata, which is unexpected.");
        }
        return builder.build();
    }

    public abstract int numHashedRowComponents();

    public abstract ValueType streamIdType();

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    public SchemaMetadataPersistence.StreamStoreCleanupMetadata persistToProto() {
        SchemaMetadataPersistence.StreamStoreCleanupV1Metadata v1Metadata = persistV1Metadata();

        return SchemaMetadataPersistence.StreamStoreCleanupMetadata.newBuilder()
                .setV1Metadata(v1Metadata)
                .build();
    }

    private SchemaMetadataPersistence.StreamStoreCleanupV1Metadata persistV1Metadata() {
        return SchemaMetadataPersistence.StreamStoreCleanupV1Metadata.newBuilder()
                .setNumHashedRowComponents(numHashedRowComponents())
                .setStreamIdType(streamIdType().persistToProto())
                .build();
    }
}
