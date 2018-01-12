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

import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;
import com.palantir.atlasdb.table.description.ValueType;

@Value.Immutable
public abstract class StreamStoreCleanupMetadata implements CleanupMetadata {
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

    private SchemaMetadataPersistence.StreamStoreCleanupMetadata persistToProto() {
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
