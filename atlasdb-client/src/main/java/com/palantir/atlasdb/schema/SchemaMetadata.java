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

import java.util.Map;

import org.immutables.value.Value;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.SchemaMetadataPersistence;
import com.palantir.common.persist.Persistable;

@Value.Immutable
public abstract class SchemaMetadata implements Persistable {
    public static final Hydrator<SchemaMetadata> HYDRATOR = input -> {
        try {
            return hydrateFromProto(SchemaMetadataPersistence.SchemaMetadata.parseFrom(input));
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    };

    public abstract Map<TableReference, SchemaDependentTableMetadata> schemaDependentTableMetadata();

    @Override
    public byte[] persistToBytes() {
        return persistToProto().toByteArray();
    }

    public SchemaMetadataPersistence.SchemaMetadata persistToProto() {
        SchemaMetadataPersistence.SchemaMetadata.Builder builder =
                SchemaMetadataPersistence.SchemaMetadata.newBuilder();
        schemaDependentTableMetadata().entrySet().forEach(entry -> builder.addTableMetadata(
                SchemaMetadataPersistence.SchemaDependentTableMetadataEntry.newBuilder()
                        .setTableReference(
                                SchemaMetadataPersistence.TableReference.newBuilder()
                                        .setNamespace(entry.getKey().getNamespace().getName())
                                        .setTableName(entry.getKey().getTablename())
                                        .build())
                        .setSchemaDependentTableMetadata(entry.getValue().persistToProto())
        ));
        return builder.build();
    }

    private static SchemaMetadata hydrateFromProto(SchemaMetadataPersistence.SchemaMetadata message) {
        ImmutableSchemaMetadata.Builder builder = ImmutableSchemaMetadata.builder();
        message.getTableMetadataList()
                .forEach(entry -> builder.putSchemaDependentTableMetadata(
                        TableReference.create(
                                Namespace.create(entry.getTableReference().getNamespace()),
                                entry.getTableReference().getTableName()),
                        SchemaDependentTableMetadata.hydrateFromProto(entry.getSchemaDependentTableMetadata())));
        return builder.build();
    }
}
