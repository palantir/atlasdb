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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CqlKeyValueServices;
import com.palantir.atlasdb.table.description.TableMetadata;

public class CqlKeyValueServicesTest {
    private static final List<TableReference> TABLE_REFERENCES = ImmutableList.of(
            TableReference.createFromFullyQualifiedName("test.test"),
            TableReference.createFromFullyQualifiedName("test.test2"),
            TableReference.createFromFullyQualifiedName("test2.test"),
            TableReference.createWithEmptyNamespace("test"),
            TableReference.fromInternalTableName("_test"));
    private static final List<byte[]> METADATAS = ImmutableList.of(
            new TableMetadata().persistToBytes(),
            AtlasDbConstants.EMPTY_TABLE_METADATA,
            AtlasDbConstants.GENERIC_TABLE_METADATA);
    private CassandraKeyValueServiceConfig mockConfig = mock(CassandraKeyValueServiceConfig.class);

    @Test
    public void generatedUuidsShouldBeDifferentForDifferentInputs() {
        Set<UUID> results = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            when(mockConfig.getKeyspaceOrThrow()).thenReturn("awesomeKeyspace" + i);
            TABLE_REFERENCES.forEach(tableRef ->
                    METADATAS.forEach(metadata ->
                    results.add(CqlKeyValueServices.getUuid(tableRef, metadata, mockConfig))));
        }
        assertThat(results.size()).isEqualTo(3 * TABLE_REFERENCES.size() * METADATAS.size());
    }

    @Test
    public void generatedUuidsShouldBeSameForSameInputs() {
        for (int i = 0; i < 3; i++) {
            when(mockConfig.getKeyspaceOrThrow()).thenReturn("awesomeKeyspace" + i);
            TABLE_REFERENCES.forEach(tableRef ->
                    METADATAS.forEach(metadata ->
                            assertThat(CqlKeyValueServices.getUuid(tableRef, metadata, mockConfig))
                                    .isEqualTo(CqlKeyValueServices.getUuid(tableRef, metadata, mockConfig))));
        }
    }
}
