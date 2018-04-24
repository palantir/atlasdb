/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.cleanup.ArbitraryCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.NullCleanupMetadata;

public class SchemaMetadataTest {
    private final SchemaDependentTableMetadata TABLE_METADATA_1 = ImmutableSchemaDependentTableMetadata.builder()
            .cleanupMetadata(new ArbitraryCleanupMetadata())
            .build();
    private final SchemaDependentTableMetadata TABLE_METADATA_2 = ImmutableSchemaDependentTableMetadata.builder()
            .cleanupMetadata(new NullCleanupMetadata())
            .build();

    @Test
    public void canSerializeAndDeserializeSchemaMetadataWithZeroTables() {
        canSerializeAndDeserializePreservingEquality(ImmutableSchemaMetadata.builder().build());
    }

    @Test
    public void canSerializeAndDeserializeSchemaMetadataWithOneTable() {
        canSerializeAndDeserializePreservingEquality(ImmutableSchemaMetadata.builder()
                .putSchemaDependentTableMetadata(
                        TableReference.create(Namespace.create("foo"), "bar"), TABLE_METADATA_1)
                .build());
    }

    @Test
    public void canSerializeAndDeserializeSchemaMetadataWithMultipleTables() {
        canSerializeAndDeserializePreservingEquality(ImmutableSchemaMetadata.builder()
                .putSchemaDependentTableMetadata(
                        TableReference.create(Namespace.create("foo"), "bar"), TABLE_METADATA_1)
                .putSchemaDependentTableMetadata(
                        TableReference.create(Namespace.create("bar"), "foo"), TABLE_METADATA_2)
                .putSchemaDependentTableMetadata(
                        TableReference.create(Namespace.create("foo"), "barbar"), TABLE_METADATA_2)
                .build());
    }

    private void canSerializeAndDeserializePreservingEquality(SchemaMetadata schemaMetadata) {
        assertThat(SchemaMetadata.HYDRATOR.hydrateFromBytes(schemaMetadata.persistToBytes()))
                .isEqualTo(schemaMetadata);
    }
}
