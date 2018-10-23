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
package com.palantir.atlasdb.schema.cleanup;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;

public class NullCleanupMetadata implements CleanupMetadata {
    public static Hydrator<NullCleanupMetadata> BYTES_HYDRATOR = message -> {
        try {
            return hydrateFromProto(SchemaMetadataPersistence.NullCleanupMetadata.parseFrom(message));
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    };

    public static NullCleanupMetadata hydrateFromProto(SchemaMetadataPersistence.NullCleanupMetadata unused) {
        // This is correct while the NullCleanupMetadata message has no fields.
        return new NullCleanupMetadata();
    }

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    public SchemaMetadataPersistence.NullCleanupMetadata persistToProto() {
        return SchemaMetadataPersistence.NullCleanupMetadata.newBuilder().build();
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other != null && this.getClass() == other.getClass());
    }

    @Override
    public int hashCode() {
        return NullCleanupMetadata.class.hashCode();
    }
}
