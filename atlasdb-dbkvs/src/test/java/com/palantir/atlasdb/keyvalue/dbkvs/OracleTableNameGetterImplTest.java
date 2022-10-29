/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.logsafe.SafeArg;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OracleTableNameGetterImplTest {
    private static final OracleDdlConfig TABLE_MAPPING_DDL_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(true)
            .build();

    private static final OracleDdlConfig NON_TABLE_MAPPING_DDL_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(false)
            .build();

    private static final Map<TableReference, String> REFS_TO_SHORT_TABLE_NAMES = ImmutableMap.of(
            TableReference.create(Namespace.create("test"), "world"), "hello",
            TableReference.createWithEmptyNamespace("hello"), "world");

    private static final Set<String> SHORT_TABLE_NAMES = new HashSet<>(REFS_TO_SHORT_TABLE_NAMES.values());
    private static final Set<TableReference> TABLE_REFERENCES = REFS_TO_SHORT_TABLE_NAMES.keySet();

    @Mock
    private ConnectionSupplier connectionSupplier;

    @Mock
    private OracleTableNameMapper tableNameMapper;

    @Mock
    private OracleTableNameUnmapper tableNameUnmapper;

    private OracleTableNameGetter tableMappingTableNameGetter;
    private OracleTableNameGetter nonTableMappingTableNameGetter;

    @Before
    public void before() {
        tableMappingTableNameGetter =
                OracleTableNameGetterImpl.createForTests(TABLE_MAPPING_DDL_CONFIG, tableNameMapper, tableNameUnmapper);
        nonTableMappingTableNameGetter = OracleTableNameGetterImpl.createForTests(
                NON_TABLE_MAPPING_DDL_CONFIG, tableNameMapper, tableNameUnmapper);
    }

    @Test
    public void getTableReferencesFromShortTableNamesTransformsUnmapperNamesWhenMappingEnabled() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(getDefaultLongTableNames());
        Set<TableReference> tableReferences = tableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                connectionSupplier, SHORT_TABLE_NAMES);

        assertThat(tableReferences).containsExactlyInAnyOrderElementsOf(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesLoadsFromUnmapperWhenMappingEnabled() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(getDefaultLongOverflowTableNames());

        Set<TableReference> tableReferences = tableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                connectionSupplier, SHORT_TABLE_NAMES);
        assertThat(tableReferences).containsExactlyInAnyOrderElementsOf(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortTableNamesTransformsProvidedNamesWhenMappingDisabled() {
        Set<TableReference> tableReferences = nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                connectionSupplier, new HashSet<>(getDefaultLongTableNames().values()));
        assertThat(tableReferences).containsExactlyInAnyOrderElementsOf(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesTransformsProvidedNamesWhenMappingDisabled() {
        Set<TableReference> tableReferences =
                nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier,
                        new HashSet<>(getDefaultLongOverflowTableNames().values()));
        assertThat(tableReferences).containsExactlyInAnyOrderElementsOf(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortTableNamesThrowsIfMappedLongNameDoesNotBeginWithPrefix() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(SHORT_TABLE_NAMES.stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.identity())));
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> tableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                TABLE_MAPPING_DDL_CONFIG.tablePrefix());
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesThrowsIfMappedLongNameDoesNotBeginWithPrefix() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(SHORT_TABLE_NAMES.stream()
                        .collect(Collectors.toMap(Functions.identity(), Functions.identity())));
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> tableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                TABLE_MAPPING_DDL_CONFIG.overflowTablePrefix());
    }

    @Test
    public void getTableReferencesFromShortTableNamesThrowsIfLongNameDoesNotBeginWithPrefix() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(Map.of());
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                NON_TABLE_MAPPING_DDL_CONFIG.tablePrefix());
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesThrowsIfLongNameDoesNotBeginWithPrefix() {
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(Map.of());
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                NON_TABLE_MAPPING_DDL_CONFIG.overflowTablePrefix());
    }

    @Test
    public void getTableReferencesFromShortTableNamesFiltersMappedNamesWhenMappingDisabled() {
        Set<String> extraTableNames = Set.of("test", "test2");
        Set<String> allTableNames = Sets.union(SHORT_TABLE_NAMES, addTablePrefix(extraTableNames));
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, allTableNames))
                .thenReturn(getDefaultLongTableNames());
        assertThat(nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, allTableNames))
                .containsExactlyInAnyOrderElementsOf(extraTableNames.stream()
                        .map(TableReference::fromInternalTableName)
                        .collect(Collectors.toSet()));
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesFiltersMappedNamesWhenMappingDisabled() {
        Set<String> extraTableNames = Set.of("test", "test2");
        Set<String> allTableNames = Sets.union(SHORT_TABLE_NAMES, addOverflowTablePrefix(extraTableNames));
        when(tableNameUnmapper.getShortToLongTableNamesFromMappingTable(connectionSupplier, allTableNames))
                .thenReturn(getDefaultLongOverflowTableNames());
        assertThat(nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, allTableNames))
                .containsExactlyInAnyOrderElementsOf(extraTableNames.stream()
                        .map(TableReference::fromInternalTableName)
                        .collect(Collectors.toSet()));
    }

    @Test
    public void getTableReferencesFromShortTableNamesPreservesCaseWhenUnmapped() {
        Set<String> expectedTableNames = Set.of("test", "TesT2");
        assertThat(nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, addTablePrefix(expectedTableNames)))
                .containsExactlyInAnyOrderElementsOf(expectedTableNames.stream()
                        .map(TableReference::fromInternalTableName)
                        .collect(Collectors.toSet()));
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesPreservesCaseWhenUnmapped() {
        Set<String> expectedTableNames = Set.of("test", "TesT2");
        assertThat(nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, addOverflowTablePrefix(expectedTableNames)))
                .containsExactlyInAnyOrderElementsOf(expectedTableNames.stream()
                        .map(TableReference::fromInternalTableName)
                        .collect(Collectors.toSet()));
    }

    private Map<String, String> getDefaultLongOverflowTableNames() {
        return REFS_TO_SHORT_TABLE_NAMES.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        entry -> tableMappingTableNameGetter.getPrefixedOverflowTableName(entry.getKey())));
    }

    private Map<String, String> getDefaultLongTableNames() {
        return REFS_TO_SHORT_TABLE_NAMES.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getValue,
                        entry -> tableMappingTableNameGetter.getPrefixedTableName(entry.getKey())));
    }

    private Set<String> addTablePrefix(Set<String> tableNames) {
        return tableNames.stream()
                .map(tableName -> TABLE_MAPPING_DDL_CONFIG.tablePrefix() + tableName)
                .collect(Collectors.toSet());
    }

    private Set<String> addOverflowTablePrefix(Set<String> tableNames) {
        return tableNames.stream()
                .map(tableName -> TABLE_MAPPING_DDL_CONFIG.overflowTablePrefix() + tableName)
                .collect(Collectors.toSet());
    }

    private static void assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
            ThrowingCallable callable, String prefix) {
        assertThatLoggableExceptionThrownBy(callable)
                .hasLogMessage("Long table name does not begin with prefix")
                .containsArgs(SafeArg.of("prefix", prefix));
    }
}
