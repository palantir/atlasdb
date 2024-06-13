/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableTableMappingCacheConfiguration;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KvTableMappingServiceTest {
    private static final TableReference FQ_TABLE = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference FQ_TABLE2 = TableReference.createFromFullyQualifiedName("test2.test2");
    private static final TableReference FQ_TABLE3 = TableReference.createFromFullyQualifiedName("test3.test3");
    private static final TableReference TABLE_EMPTY_NAMESPACE = TableReference.createWithEmptyNamespace("test");
    private static final TableReference UNCACHEABLE_TABLE_1 =
            TableReference.createFromFullyQualifiedName("cant.cachethis");
    private static final TableReference UNCACHEABLE_TABLE_2 =
            TableReference.createFromFullyQualifiedName("cant.cachethistoo");
    private static final Set<TableReference> UNCACHEABLE_TABLES =
            ImmutableSet.of(UNCACHEABLE_TABLE_1, UNCACHEABLE_TABLE_2);

    private AtomicLong counter;
    private KeyValueService kvs;
    private TableMappingService tableMapping;

    @BeforeEach
    public void setup() {
        kvs = spy(new InMemoryKeyValueService(false));
        counter = new AtomicLong(0);
        tableMapping = createTableMappingService();
        tableMapping.addTable(FQ_TABLE);
    }

    @Test
    public void getMappedTableNameReturnsShortTableName() throws TableMappingNotFoundException {
        assertThat(tableMapping.getMappedTableName(FQ_TABLE)).isEqualTo(shortTableRefForNumber(1));
    }

    @Test
    public void getMappedTableNameThrowsOnNonExistingTableName() throws TableMappingNotFoundException {
        assertThatThrownBy(() -> tableMapping.getMappedTableName(FQ_TABLE2))
                .as("Table that was never added")
                .isInstanceOf(TableMappingNotFoundException.class);
    }

    @Test
    public void getMappedTableNameIsIdentityForTablesWithoutNamespace() throws TableMappingNotFoundException {
        assertThat(tableMapping.getMappedTableName(TABLE_EMPTY_NAMESPACE)).isEqualTo(TABLE_EMPTY_NAMESPACE);
    }

    @Test
    public void getMappedTableNamesThrowsForInvalidShortTableName() {
        insertInvalidShortTableNameIntoKvs();
        assertThatThrownBy(() -> tableMapping.getMappedTableName(FQ_TABLE2))
                .as("Invalid short table name in KVS")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canAddFullyQualifiedTable() throws TableMappingNotFoundException {
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        assertThat(tableMapping.getMappedTableName(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));

        verify(kvs, times(2)).putUnlessExists(eq(AtlasDbConstants.NAMESPACE_TABLE), anyMap());
    }

    @Test
    public void addingTablesIsIdempotent() throws TableMappingNotFoundException {
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        assertThat(tableMapping.getMappedTableName(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));

        // implementation detail: the first time we reattempt, we will try to CAS, fail, and reload the mapping
        verify(kvs, times(3)).putUnlessExists(eq(AtlasDbConstants.NAMESPACE_TABLE), anyMap());
    }

    @Test
    public void addingTableWithoutNamespaceIsNoop() throws TableMappingNotFoundException {
        assertThat(tableMapping.addTable(TABLE_EMPTY_NAMESPACE)).isEqualTo(TABLE_EMPTY_NAMESPACE);

        verify(kvs, times(1)).putUnlessExists(eq(AtlasDbConstants.NAMESPACE_TABLE), anyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canRemoveFullyQualifiedTables() throws TableMappingNotFoundException {
        tableMapping.addTable(FQ_TABLE2);
        tableMapping.removeTables(ImmutableSet.of(FQ_TABLE, FQ_TABLE2, TABLE_EMPTY_NAMESPACE));

        assertThatThrownBy(() -> tableMapping.getMappedTableName(FQ_TABLE))
                .as("Removed table")
                .isInstanceOf(TableMappingNotFoundException.class);
        assertThatThrownBy(() -> tableMapping.getMappedTableName(FQ_TABLE2))
                .as("Removed table")
                .isInstanceOf(TableMappingNotFoundException.class);
        assertThat(tableMapping.getMappedTableName(TABLE_EMPTY_NAMESPACE)).isEqualTo(TABLE_EMPTY_NAMESPACE);

        verify(kvs, atLeast(1)).delete(eq(AtlasDbConstants.NAMESPACE_TABLE), any(Multimap.class));
    }

    @Test
    public void removedTablesCanBeReadded() throws TableMappingNotFoundException {
        tableMapping.removeTable(FQ_TABLE);
        tableMapping.addTable(FQ_TABLE);

        assertThat(tableMapping.getMappedTableName(FQ_TABLE)).isEqualTo(shortTableRefForNumber(2));
    }

    @Test
    public void removeTableIsIdempotent() throws TableMappingNotFoundException {
        tableMapping.removeTable(FQ_TABLE);
        tableMapping.removeTable(FQ_TABLE);

        assertThatThrownBy(() -> tableMapping.getMappedTableName(FQ_TABLE))
                .as("Removed table")
                .isInstanceOf(TableMappingNotFoundException.class);
    }

    @Test
    public void generateMapToFullTableNamesReturnsKnownMappingsAndIdentityForOthers() {
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        tableMapping.removeTable(FQ_TABLE2);

        Map<TableReference, TableReference> result = tableMapping.generateMapToFullTableNames(ImmutableSet.of(
                shortTableRefForNumber(1),
                shortTableRefForNumber(2),
                shortTableRefForNumber(3),
                FQ_TABLE2,
                TABLE_EMPTY_NAMESPACE));

        Map<TableReference, TableReference> expected = ImmutableMap.of(
                shortTableRefForNumber(1),
                FQ_TABLE,
                shortTableRefForNumber(2),
                shortTableRefForNumber(2),
                shortTableRefForNumber(3),
                shortTableRefForNumber(3),
                FQ_TABLE2,
                FQ_TABLE2,
                TABLE_EMPTY_NAMESPACE,
                TABLE_EMPTY_NAMESPACE);

        assertThat(result).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    public void generateMapToFullTableNamesCachesMapping() {
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));

        int queryIterations = 100;
        for (int iteration = 0; iteration < queryIterations; iteration++) {
            Map<TableReference, TableReference> result = tableMapping.generateMapToFullTableNames(ImmutableSet.of(
                    shortTableRefForNumber(1),
                    shortTableRefForNumber(2),
                    shortTableRefForNumber(3),
                    FQ_TABLE2,
                    TABLE_EMPTY_NAMESPACE));

            Map<TableReference, TableReference> expected = ImmutableMap.of(
                    shortTableRefForNumber(1),
                    FQ_TABLE,
                    shortTableRefForNumber(2),
                    FQ_TABLE2,
                    shortTableRefForNumber(3),
                    shortTableRefForNumber(3),
                    FQ_TABLE2,
                    FQ_TABLE2,
                    TABLE_EMPTY_NAMESPACE,
                    TABLE_EMPTY_NAMESPACE);

            assertThat(result).containsExactlyInAnyOrderEntriesOf(expected);
        }
        // Once on startup (where it's empty), and once on the first query, but not after that.
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.NAMESPACE_TABLE), any(), anyLong());
    }

    @Test
    public void mapToShortTableNamesSucceedsWhenAllTablesExist() throws TableMappingNotFoundException {
        tableMapping.addTable(FQ_TABLE2);
        Map<TableReference, Long> result = tableMapping.mapToShortTableNames(ImmutableMap.of(
                FQ_TABLE, 11L,
                FQ_TABLE2, 22L,
                TABLE_EMPTY_NAMESPACE, 33L));

        Map<TableReference, Long> expected = ImmutableMap.of(
                shortTableRefForNumber(1), 11L, shortTableRefForNumber(2), 22L, TABLE_EMPTY_NAMESPACE, 33L);

        assertThat(result).containsAllEntriesOf(expected);
        assertThat(expected).containsAllEntriesOf(result);
    }

    @Test
    public void mapToShortTableNamesThrowsOnNonExistentTable() {
        assertThatThrownBy(() -> tableMapping.mapToShortTableNames(ImmutableMap.of(FQ_TABLE2, 22L)))
                .as("Table was never added")
                .isInstanceOf(TableMappingNotFoundException.class);
    }

    @Test
    public void mapToShortTableNamesThrowsOnInvalidShortTableName() {
        insertInvalidShortTableNameIntoKvs();
        assertThatThrownBy(() -> tableMapping.mapToShortTableNames(ImmutableMap.of(FQ_TABLE2, 22L)))
                .as("Invalid short table name in KVS")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void tableNamesCanBeCached() throws TableMappingNotFoundException {
        TableReference mappedTable = tableMapping.getMappedTableName(FQ_TABLE);
        for (int iteration = 0; iteration < 100; iteration++) {
            assertThat(tableMapping.getMappedTableName(FQ_TABLE)).isEqualTo(mappedTable);
        }

        // Once on startup (where it's empty), and once on the first query, but not after that.
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.NAMESPACE_TABLE), any(), anyLong());
    }

    @Test
    public void loadingTableNamesIntoCacheAppliesAcrossDistinctCacheableTables() throws TableMappingNotFoundException {
        // This ordering looks strange but is intentional: we need to add the new tables before calling get
        TableReference mappedTableTwo = tableMapping.addTable(FQ_TABLE2);
        TableReference mappedTableThree = tableMapping.addTable(FQ_TABLE3);
        TableReference mappedTableOne = tableMapping.getMappedTableName(FQ_TABLE);

        assertThat(tableMapping.getMappedTableName(FQ_TABLE2)).isEqualTo(mappedTableTwo);
        assertThat(tableMapping.getMappedTableName(FQ_TABLE3)).isEqualTo(mappedTableThree);

        assertThat(ImmutableSet.of(mappedTableOne, mappedTableTwo, mappedTableThree))
                .as("mapped tables should all be different")
                .hasSize(3);

        // Once on startup (where it's empty), and once on the first query (loading everything!), but not after that.
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.NAMESPACE_TABLE), any(), anyLong());
    }

    @Test
    public void loadingUncacheableTableDetectsEntryModifiedInKvsUnderneathUs() throws TableMappingNotFoundException {
        TableReference mappedTableFromOriginalService = tableMapping.addTable(UNCACHEABLE_TABLE_1);

        TableMappingService anotherService = createTableMappingService();
        anotherService.removeTable(UNCACHEABLE_TABLE_1);
        anotherService.addTable(UNCACHEABLE_TABLE_1);

        TableReference mappedTableFromAnotherService = anotherService.getMappedTableName(UNCACHEABLE_TABLE_1);
        assertThat(mappedTableFromAnotherService)
                .as("two table mapping services with the same counter should generate different short names")
                .isNotEqualTo(mappedTableFromOriginalService);

        assertThat(tableMapping.getMappedTableName(UNCACHEABLE_TABLE_1))
                .as("we should reload the mapping from the KVS and detect that it was changed underneath us")
                .isEqualTo(mappedTableFromAnotherService);
    }

    @Test
    public void loadingUncacheableTableDetectsEntryDeletedInKvsUnderneathUs() {
        tableMapping.addTable(UNCACHEABLE_TABLE_1);

        TableMappingService anotherService = createTableMappingService();
        anotherService.removeTable(UNCACHEABLE_TABLE_1);
        assertThatThrownBy(() -> tableMapping.getMappedTableName(UNCACHEABLE_TABLE_1))
                .isInstanceOf(TableMappingNotFoundException.class)
                .hasMessage("Unable to resolve mapping for table reference " + UNCACHEABLE_TABLE_1);
    }

    @Test
    public void loadingUncacheableTableDoesNotInvalidateCacheForCacheableTables() throws TableMappingNotFoundException {
        TableReference mappedUncacheableTable = tableMapping.addTable(UNCACHEABLE_TABLE_1);
        TableReference mappedBaseTable = tableMapping.getMappedTableName(FQ_TABLE);
        int queryIterations = 100;
        for (int iteration = 0; iteration < queryIterations; iteration++) {
            assertThat(tableMapping.getMappedTableName(FQ_TABLE)).isEqualTo(mappedBaseTable);
            assertThat(tableMapping.getMappedTableName(UNCACHEABLE_TABLE_1)).isEqualTo(mappedUncacheableTable);
        }

        verify(kvs, times(queryIterations)).get(eq(AtlasDbConstants.NAMESPACE_TABLE), any());
        // Once on startup (where it's empty), and once on the first query, but not after that.
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.NAMESPACE_TABLE), any(), anyLong());
    }

    @Test
    public void mapToShortTableNamesMapsFullyQualifiedNamesToShortNames() throws TableMappingNotFoundException {
        tableMapping.addTable(FQ_TABLE2);

        Map<TableReference, Integer> shortTableNamesAsKeys = tableMapping.mapToShortTableNames(ImmutableMap.of(
                FQ_TABLE, 1,
                FQ_TABLE2, 2));
        assertThat(shortTableNamesAsKeys)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        tableMapping.getMappedTableName(FQ_TABLE), 1,
                        tableMapping.getMappedTableName(FQ_TABLE2), 2));
    }

    @Test
    public void mapToShortTableNamesThrowsIfKeyNotFoundInTableMap() {
        assertThatThrownBy(() -> tableMapping.mapToShortTableNames(ImmutableMap.of(FQ_TABLE, 1, FQ_TABLE2, 2)))
                .isInstanceOf(TableMappingNotFoundException.class)
                .hasMessage("Unable to resolve mapping for table reference " + FQ_TABLE2);
    }

    @Test
    public void mapToShortTableNamesReadsFromCache() throws TableMappingNotFoundException {
        TableReference mappedTableTwo = tableMapping.addTable(FQ_TABLE2);
        TableReference mappedTableThree = tableMapping.addTable(FQ_TABLE3);

        int queryIterations = 100;
        for (int iteration = 0; iteration < queryIterations; iteration++) {
            assertThat(tableMapping.mapToShortTableNames(ImmutableMap.of(FQ_TABLE2, 2, FQ_TABLE3, 3)))
                    .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(mappedTableTwo, 2, mappedTableThree, 3));
        }

        // Once on startup (where it's empty), and once on the first query, but not after that.
        verify(kvs, times(2)).getRange(eq(AtlasDbConstants.NAMESPACE_TABLE), any(), anyLong());
    }

    @Test
    public void mapToShortTableNamesConsultsKeyValueServiceForUncacheableTables() throws TableMappingNotFoundException {
        tableMapping.addTable(UNCACHEABLE_TABLE_1);
        tableMapping.addTable(UNCACHEABLE_TABLE_2);

        TableMappingService anotherService = createTableMappingService();
        anotherService.removeTable(UNCACHEABLE_TABLE_1);
        TableReference newMappedUncacheableTableOne = anotherService.addTable(UNCACHEABLE_TABLE_1);
        anotherService.removeTable(UNCACHEABLE_TABLE_2);
        TableReference newMappedUncacheableTableTwo = anotherService.addTable(UNCACHEABLE_TABLE_2);

        Map<TableReference, Integer> shortTableNamesAsKeys = tableMapping.mapToShortTableNames(ImmutableMap.of(
                FQ_TABLE, 1,
                UNCACHEABLE_TABLE_1, 2,
                UNCACHEABLE_TABLE_2, 3));
        assertThat(shortTableNamesAsKeys)
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        tableMapping.getMappedTableName(FQ_TABLE),
                        1,
                        newMappedUncacheableTableOne,
                        2,
                        newMappedUncacheableTableTwo,
                        3));
    }

    @Test
    public void generateMapToFullTableNamesHandlesChangesInTheUnderlyingDatabase() {
        assertThat(tableMapping.addTable(FQ_TABLE2)).isEqualTo(shortTableRefForNumber(2));
        assertThat(tableMapping.addTable(UNCACHEABLE_TABLE_1)).isEqualTo(shortTableRefForNumber(3));
        assertThat(tableMapping.addTable(UNCACHEABLE_TABLE_2)).isEqualTo(shortTableRefForNumber(4));

        TableMappingService anotherService = createTableMappingService();
        anotherService.removeTable(UNCACHEABLE_TABLE_1);
        assertThat(anotherService.addTable(UNCACHEABLE_TABLE_1)).isEqualTo(shortTableRefForNumber(5));

        Map<TableReference, TableReference> result = tableMapping.generateMapToFullTableNames(ImmutableSet.of(
                shortTableRefForNumber(1),
                shortTableRefForNumber(2),
                shortTableRefForNumber(3),
                shortTableRefForNumber(4),
                shortTableRefForNumber(5)));

        Map<TableReference, TableReference> expected = ImmutableMap.of(
                shortTableRefForNumber(1),
                FQ_TABLE,
                shortTableRefForNumber(2),
                FQ_TABLE2,
                shortTableRefForNumber(3),
                shortTableRefForNumber(3),
                shortTableRefForNumber(4),
                UNCACHEABLE_TABLE_2,
                shortTableRefForNumber(5),
                UNCACHEABLE_TABLE_1);

        assertThat(result).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private static TableReference shortTableRefForNumber(long sequenceNumber) {
        return TableReference.createWithEmptyNamespace(AtlasDbConstants.NAMESPACE_PREFIX + sequenceNumber);
    }

    private void insertInvalidShortTableNameIntoKvs() {
        Cell keyCell = KvTableMappingService.getKeyCellForTable(FQ_TABLE2);
        byte[] nameAsBytes = PtBytes.toBytes("fake/table_name");
        kvs.putUnlessExists(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMap.of(keyCell, nameAsBytes));
    }

    private KvTableMappingService createTableMappingService() {
        return KvTableMappingService.createWithCustomNamespaceValidation(
                kvs,
                counter::incrementAndGet,
                Namespace.STRICTLY_CHECKED_NAME,
                ImmutableTableMappingCacheConfiguration.builder()
                        .cacheableTablePredicate(tableRef -> !UNCACHEABLE_TABLES.contains(tableRef))
                        .build());
    }
}
