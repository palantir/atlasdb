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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyvalueServiceTestUtils.clearOutMetadataTable;
import static com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyvalueServiceTestUtils.insertMetadataIntoLegacyCell;
import static com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyvalueServiceTestUtils.originalMetadata;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraStrangeBehaviourTest {
    private static final TableReference UPPER_UPPER = TableReference.createFromFullyQualifiedName("TEST.TABLE");
    private static final TableReference LOWER_UPPER = TableReference.createFromFullyQualifiedName("test.TABLE");
    private static final TableReference LOWER_LOWER = TableReference.createFromFullyQualifiedName("test.table");
    private static final List<TableReference> TABLES = ImmutableList.of(UPPER_UPPER, LOWER_UPPER, LOWER_LOWER);
    public static final byte[] BYTE_ARRAY = new byte[] {1};
    public static final byte[] SECOND_BYTE_ARRAY = new byte[] {2};
    public static final Cell CELL = Cell.create(BYTE_ARRAY, BYTE_ARRAY);

    private KeyValueService kvs = CASSANDRA.getDefaultKvs();

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @After
    public void cleanup() {
        kvs.dropTables(kvs.getAllTableNames());
        kvs.truncateTable(AtlasDbConstants.DEFAULT_METADATA_TABLE);
    }

    @Test
    public void getMetadataForTablesReturnsWithCorrectCapitalization() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertThat(kvs.getMetadataForTables().keySet())
                .contains(UPPER_UPPER)
                .doesNotContain(LOWER_LOWER);
    }

    @Test
    public void droppedTableHasNoObservableMetadata() {
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(LOWER_UPPER);

        assertThat(kvs.getMetadataForTables()).isEmpty();
        assertThat(kvs.getMetadataForTable(LOWER_UPPER)).isEmpty();
    }

    @Test
    public void nonExistentTablesWithMetadataDoNotAppearInGetMetadataForTables() {
        insertMetadataIntoNewCell(LOWER_UPPER);

        assertThat(kvs.getMetadataForTables()).isEmpty();
        assertThat(kvs.getMetadataForTable(LOWER_UPPER)).contains(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void getMetadataReturnsResultFromNewMetadataCellOnConflict() {
        kvs.createTable(LOWER_UPPER, originalMetadata());
        insertMetadataIntoLegacyCell(kvs, LOWER_UPPER);

        assertThat(kvs.getMetadataForTables().get(LOWER_UPPER)).contains(originalMetadata());
        assertThat(kvs.getMetadataForTable(LOWER_UPPER)).contains(originalMetadata());
    }


    @Test
    public void droppingTablesCleansUpLegacyMetadataAndDoesNotAffectOtherTables() {
        TableReference longerInRange = TableReference.createFromFullyQualifiedName("test.TABLEs");
        TableReference shorterInRange = TableReference.createFromFullyQualifiedName("test.TABL");
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(longerInRange, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(shorterInRange, AtlasDbConstants.GENERIC_TABLE_METADATA);

        clearOutMetadataTable(kvs);

        insertMetadataIntoNewCell(LOWER_UPPER);
        insertMetadataIntoLegacyCell(kvs, LOWER_UPPER);
        insertMetadataIntoLegacyCell(kvs, UPPER_UPPER);
        insertMetadataIntoLegacyCell(kvs, longerInRange);
        insertMetadataIntoLegacyCell(kvs, shorterInRange);

        kvs.dropTable(LOWER_UPPER);

        assertThat(kvs.getMetadataForTables().keySet()).containsExactlyInAnyOrder(longerInRange, shorterInRange);
        assertThat(kvs.getMetadataForTable(LOWER_UPPER)).isEmpty();
    }

    /**
     * Tests below document strange or inconsistent behaviour. We should decide if and how to fix it.
     */

    @Test
    public void createTablesWithDifferentCapitalizationCreatesInCassandraAndDoesNotUpdateMetadata() {
        createTablesIgnoringException();
        assertThat(kvs.getAllTableNames()).containsExactlyInAnyOrderElementsOf(TABLES);
        assertThat(kvs.getMetadataForTables()).isEmpty();
    }

    @Test
    public void tablesWithDifferentCapitalizationClaimToHaveMetadata() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        TABLES.forEach(table -> assertThat(kvs.getMetadataForTable(table)).isNotEmpty());
        assertThat(kvs.getMetadataForTable(TableReference.createFromFullyQualifiedName("other.table"))).isEmpty();
    }

    @Test
    public void tableReferencesAreCaseSensitiveForPutAndGet() {
        createTablesIgnoringException();

        kvs.put(UPPER_UPPER, ImmutableMap.of(CELL, BYTE_ARRAY), 1);
        kvs.put(LOWER_LOWER, ImmutableMap.of(CELL, SECOND_BYTE_ARRAY), 1);

        assertThat(kvs.get(UPPER_UPPER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(BYTE_ARRAY);
        assertThat(kvs.get(LOWER_UPPER, ImmutableMap.of(CELL, 2L))).doesNotContainKey(CELL);
        assertThat(kvs.get(LOWER_LOWER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(SECOND_BYTE_ARRAY);
    }

    @Test
    public void tableReferencesAreCaseSensitiveForDrop() {
        kvs.createTable(LOWER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.dropTable(LOWER_LOWER);

        assertThat(kvs.getAllTableNames()).containsExactly(LOWER_UPPER);
        assertThat(kvs.getMetadataForTable(LOWER_UPPER)).contains(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void tableCreationAppearsToSucceedButIsNoop() {
        kvs.createTable(UPPER_UPPER, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(LOWER_LOWER, AtlasDbConstants.GENERIC_TABLE_METADATA);

        kvs.put(UPPER_UPPER, ImmutableMap.of(CELL, BYTE_ARRAY), 1);
        assertThat(kvs.get(UPPER_UPPER, ImmutableMap.of(CELL, 2L)).get(CELL).getContents()).contains(BYTE_ARRAY);

        assertThatThrownBy(() -> kvs.get(LOWER_LOWER, ImmutableMap.of(CELL, 2L)))
                .isInstanceOf(RetryLimitReachedException.class);

        assertThatThrownBy(() -> kvs.put(LOWER_LOWER, ImmutableMap.of(CELL, SECOND_BYTE_ARRAY), 1))
                .isInstanceOf(RetryLimitReachedException.class);
    }

    private void createTablesIgnoringException() {
        assertThatThrownBy(() -> kvs.createTables(
                TABLES.stream().collect(Collectors.toMap(x -> x, no -> AtlasDbConstants.GENERIC_TABLE_METADATA))))
                .isInstanceOf(IllegalStateException.class);
    }

    private void insertMetadataIntoNewCell(TableReference tableRef) {
        Cell metadataCell = CassandraKeyValueServices.getMetadataCell(tableRef);
        kvs.put(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableMap.of(metadataCell, AtlasDbConstants.GENERIC_TABLE_METADATA),
                System.currentTimeMillis());
    }
}
