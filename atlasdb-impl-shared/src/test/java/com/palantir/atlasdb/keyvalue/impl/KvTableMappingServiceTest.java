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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.exception.TableMappingNotFoundException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class KvTableMappingServiceTest {
    private static final TableReference FQ_TABLE = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference FQ_TABLE2 = TableReference.createFromFullyQualifiedName("test2.test2");
    private static final TableReference TABLE_EMPTY_NAMESPACE = TableReference.createWithEmptyNamespace("test");

    private KeyValueService kvs;
    private TableMappingService tableMapping;

    @Before
    public void setup() {
        kvs = spy(new InMemoryKeyValueService(false));
        AtomicLong counter = new AtomicLong(0);
        tableMapping = KvTableMappingService.create(kvs, counter::incrementAndGet);
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

        assertThat(result).containsAllEntriesOf(expected);
        assertThat(expected).containsAllEntriesOf(result);
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

    private static TableReference shortTableRefForNumber(long sequenceNumber) {
        return TableReference.createWithEmptyNamespace(AtlasDbConstants.NAMESPACE_PREFIX + sequenceNumber);
    }

    private void insertInvalidShortTableNameIntoKvs() {
        Cell keyCell = KvTableMappingService.getKeyCellForTable(FQ_TABLE2);
        byte[] nameAsBytes = PtBytes.toBytes("fake/table_name");
        kvs.putUnlessExists(AtlasDbConstants.NAMESPACE_TABLE, ImmutableMap.of(keyCell, nameAsBytes));
    }
}
