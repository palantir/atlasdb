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

package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.blob.BlobSchema;
import com.palantir.atlasdb.schema.CleanupMetadataResource;
import com.palantir.atlasdb.schema.SerializableCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.TodoSchema;

public class SchemaMetadataServiceEteTest {
    private final CleanupMetadataResource resource = EteSetup.createClientToSingleNode(CleanupMetadataResource.class);

    @Test
    public void shouldBeAbleToRetrieveSchemaMetadataForTodoTable() {
        assertThat(getSerializableCleanupMetadataAndAssertPresent(TodoSchema.getSchema(), "default.todo"))
                .satisfies(metadata -> assertThat(metadata.cleanupMetadataType())
                        .isEqualTo(SerializableCleanupMetadata.NULL_TYPE));
    }

    @Test
    public void shouldBeAbleToRetrieveSchemaMetadataForStreamStoreIndexTables() {
        assertThat(getSerializableCleanupMetadataAndAssertPresent(
                BlobSchema.getSchema(), StreamTableType.INDEX.getTableName("blob.data")))
                .satisfies(metadata -> {
                    assertThat(metadata.cleanupMetadataType()).isEqualTo(SerializableCleanupMetadata.STREAM_STORE_TYPE);
                    assertThat(metadata.numHashedRowComponents()).contains(2);
                    assertThat(metadata.streamIdType()).contains(ValueType.VAR_LONG.name());
                });

        assertThat(getSerializableCleanupMetadataAndAssertPresent(
                BlobSchema.getSchema(), StreamTableType.INDEX.getTableName("blob.hotspottyData")))
                .satisfies(metadata -> {
                    assertThat(metadata.cleanupMetadataType()).isEqualTo(SerializableCleanupMetadata.STREAM_STORE_TYPE);
                    assertThat(metadata.numHashedRowComponents()).contains(0);
                    assertThat(metadata.streamIdType()).contains(ValueType.VAR_SIGNED_LONG.name());
                });
    }

    @Test
    public void shouldBeAbleToRetrieveSchemaMetadataForStreamStoreValueTables() {
        assertThat(getSerializableCleanupMetadataAndAssertPresent(
                BlobSchema.getSchema(), StreamTableType.VALUE.getTableName("blob.data")))
                .satisfies(metadata -> assertThat(metadata.cleanupMetadataType())
                        .isEqualTo(SerializableCleanupMetadata.NULL_TYPE));

        assertThat(getSerializableCleanupMetadataAndAssertPresent(
                BlobSchema.getSchema(), StreamTableType.VALUE.getTableName("blob.hotspottyData")))
                .satisfies(metadata -> assertThat(metadata.cleanupMetadataType())
                        .isEqualTo(SerializableCleanupMetadata.NULL_TYPE));
    }

    @Test
    public void shouldBeAbleToRetrieveSchemaMetadataForTablesWithCleanupTasks() {
        assertThat(getSerializableCleanupMetadataAndAssertPresent(BlobSchema.getSchema(), "blob.auditedData"))
                .satisfies(metadata -> assertThat(metadata.cleanupMetadataType())
                        .isEqualTo(SerializableCleanupMetadata.ARBITRARY_TYPE));
    }

    private SerializableCleanupMetadata getSerializableCleanupMetadataAndAssertPresent(
            Schema sourceSchema,
            String fullyQualifiedTableName) {
        return resource.get(sourceSchema.getName(), fullyQualifiedTableName)
                .orElseThrow(() -> new IllegalStateException("Could not find metadata for schema " + sourceSchema
                        + " and table " + fullyQualifiedTableName));
    }
}
