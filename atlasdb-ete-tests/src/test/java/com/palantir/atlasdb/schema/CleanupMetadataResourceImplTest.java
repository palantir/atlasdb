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
package com.palantir.atlasdb.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.metadata.SchemaMetadataService;
import com.palantir.atlasdb.table.description.ValueType;

public class CleanupMetadataResourceImplTest {
    private static final String SCHEMA_1 = "schema";
    private static final String SCHEMA_2 = "anotherSchema";
    private static final Namespace NAMESPACE_1 = Namespace.DEFAULT_NAMESPACE;
    private static final Namespace NAMESPACE_2 = Namespace.create("anotherNamespace");
    private static final String TABLENAME_1 = "foo";
    private static final String TABLENAME_2 = "bar";

    private static final TableReference TABLE_REFERENCE_1 = TableReference.create(
            NAMESPACE_1, TABLENAME_1);
    private static final TableReference TABLE_REFERENCE_2 = TableReference.create(
            NAMESPACE_2, TABLENAME_1);

    private static final SchemaMetadata SCHEMA_METADATA_1 = ImmutableSchemaMetadata
            .builder()
            .putSchemaDependentTableMetadata(TABLE_REFERENCE_1, ImmutableSchemaDependentTableMetadata.builder()
                    .cleanupMetadata(ImmutableStreamStoreCleanupMetadata.builder()
                            .numHashedRowComponents(1)
                            .streamIdType(ValueType.VAR_LONG)
                            .build()).build()).build();
    private static final SchemaMetadata SCHEMA_METADATA_2 = ImmutableSchemaMetadata
            .builder()
            .putSchemaDependentTableMetadata(TABLE_REFERENCE_2, ImmutableSchemaDependentTableMetadata.builder()
                    .cleanupMetadata(new ArbitraryCleanupMetadata())
                    .build()).build();
    private static final SerializableCleanupMetadata SERIALIZABLE_CLEANUP_METADATA_1 =
            ImmutableSerializableCleanupMetadata.builder()
                    .cleanupMetadataType(SerializableCleanupMetadata.STREAM_STORE_TYPE)
                    .numHashedRowComponents(1)
                    .streamIdType(ValueType.VAR_LONG.name())
                    .build();
    private static final SerializableCleanupMetadata SERIALIZABLE_CLEANUP_METADATA_2 =
            ImmutableSerializableCleanupMetadata.builder()
                    .cleanupMetadataType(SerializableCleanupMetadata.ARBITRARY_TYPE)
                    .build();

    private final SchemaMetadataService mockSchemaMetadataService = mock(SchemaMetadataService.class);

    private final CleanupMetadataResource cleanupMetadataResource
            = new CleanupMetadataResourceImpl(() -> mockSchemaMetadataService);

    @Test
    public void canRetrieveSchemaMetadataIfAvailable() {
        when(mockSchemaMetadataService.loadSchemaMetadata(SCHEMA_1))
                .thenReturn(Optional.of(SCHEMA_METADATA_1));

        assertThat(cleanupMetadataResource.get(SCHEMA_1, TABLE_REFERENCE_1.getQualifiedName()))
                .contains(SERIALIZABLE_CLEANUP_METADATA_1);
    }

    @Test
    public void returnsEmptyIfSchemaMetadataNotAvailable() {
        when(mockSchemaMetadataService.loadSchemaMetadata("notThere"))
                .thenReturn(Optional.empty());
        assertThat(cleanupMetadataResource.get("notThere", "something")).isEmpty();
    }

    @Test
    public void canDistinguishBetweenSchemas() {
        when(mockSchemaMetadataService.loadSchemaMetadata(SCHEMA_1))
                .thenReturn(Optional.of(SCHEMA_METADATA_1));
        when(mockSchemaMetadataService.loadSchemaMetadata(SCHEMA_2))
                .thenReturn(Optional.of(SCHEMA_METADATA_2));

        assertThat(cleanupMetadataResource.get(SCHEMA_1, TABLE_REFERENCE_1.getQualifiedName()))
                .contains(SERIALIZABLE_CLEANUP_METADATA_1);
        assertThat(cleanupMetadataResource.get(SCHEMA_2, TABLE_REFERENCE_2.getQualifiedName()))
                .contains(SERIALIZABLE_CLEANUP_METADATA_2);
    }

    @Test
    public void canDistinguishBetweenNamespaces() {
        when(mockSchemaMetadataService.loadSchemaMetadata(SCHEMA_1))
                .thenReturn(Optional.of(SCHEMA_METADATA_1));
        assertThat(cleanupMetadataResource.get(
                SCHEMA_1, TableReference.create(NAMESPACE_1, TABLENAME_2).getQualifiedName())).isEmpty();
    }

    @Test
    public void canDistinguishBetweenTableNames() {
        when(mockSchemaMetadataService.loadSchemaMetadata(SCHEMA_1))
                .thenReturn(Optional.of(SCHEMA_METADATA_1));
        assertThat(cleanupMetadataResource.get(SCHEMA_1, TABLE_REFERENCE_2.getQualifiedName())).isEmpty();
    }
}
