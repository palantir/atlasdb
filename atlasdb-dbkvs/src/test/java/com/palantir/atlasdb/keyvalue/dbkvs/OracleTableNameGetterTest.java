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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.logsafe.SafeArg;
import java.util.Set;
import java.util.stream.Collectors;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OracleTableNameGetterTest {
    private static final OracleDdlConfig TABLE_MAPPING_DDL_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(true)
            .build();

    private static final OracleDdlConfig NON_TABLE_MAPPING_DDL_CONFIG = ImmutableOracleDdlConfig.builder()
            .overflowMigrationState(OverflowMigrationState.UNSTARTED)
            .useTableMapping(false)
            .build();

    private static final Set<String> SHORT_TABLE_NAMES = ImmutableSet.of("shortNameOne", "shortNameTwo");
    private static final Set<TableReference> TABLE_REFERENCES = ImmutableSet.of(
            TableReference.create(Namespace.create("test"), "world"), TableReference.createWithEmptyNamespace("hello"));

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
                OracleTableNameGetter.createForTests(TABLE_MAPPING_DDL_CONFIG, tableNameMapper, tableNameUnmapper);
        nonTableMappingTableNameGetter =
                OracleTableNameGetter.createForTests(NON_TABLE_MAPPING_DDL_CONFIG, tableNameMapper, tableNameUnmapper);
    }

    @Test
    public void getTableReferencesFromShortTableNamesTransformsUnmapperNamesWhenMappingEnabled()
            throws TableMappingNotFoundException {
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(getLongTableNames());
        Set<TableReference> tableReferences = tableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                connectionSupplier, SHORT_TABLE_NAMES);

        assertThat(tableReferences).isEqualTo(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesLoadsFromUnmapperWhenMappingEnabled()
            throws TableMappingNotFoundException {
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(getLongOverflowTableNames());

        Set<TableReference> tableReferences = tableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                connectionSupplier, SHORT_TABLE_NAMES);
        assertThat(tableReferences).isEqualTo(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortTableNamesTransformsProvidedNamesWhenMappingDisabled()
            throws TableMappingNotFoundException {
        Set<TableReference> tableReferences = nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                connectionSupplier, getLongTableNames());
        assertThat(tableReferences).isEqualTo(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesTransformsProvidedNamesWhenMappingDisabled()
            throws TableMappingNotFoundException {
        Set<TableReference> tableReferences =
                nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, getLongOverflowTableNames());
        assertThat(tableReferences).isEqualTo(TABLE_REFERENCES);
    }

    @Test
    public void getTableReferencesFromShortTableNamesThrowsIfMappingDoesNotExist()
            throws TableMappingNotFoundException {
        TableMappingNotFoundException tableMappingNotFoundException = new TableMappingNotFoundException("Propagated!");
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenThrow(tableMappingNotFoundException);
        assertThatThrownBy(() -> tableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES))
                .isEqualTo(tableMappingNotFoundException);
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesThrowsIfMappingDoesNotExist()
            throws TableMappingNotFoundException {
        TableMappingNotFoundException tableMappingNotFoundException = new TableMappingNotFoundException("Propagated!");
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenThrow(tableMappingNotFoundException);
        assertThatThrownBy(() -> tableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES))
                .isEqualTo(tableMappingNotFoundException);
    }

    @Test
    public void getTableReferencesFromShortTableNamesThrowsIfMappedLongNameDoesNotBeginWithPrefix()
            throws TableMappingNotFoundException {
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(SHORT_TABLE_NAMES);
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> tableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                TABLE_MAPPING_DDL_CONFIG.tablePrefix());
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesThrowsIfMappedLongNameDoesNotBeginWithPrefix()
            throws TableMappingNotFoundException {
        when(tableNameUnmapper.getLongTableNamesFromMappingTable(connectionSupplier, SHORT_TABLE_NAMES))
                .thenReturn(SHORT_TABLE_NAMES);
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> tableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                TABLE_MAPPING_DDL_CONFIG.overflowTablePrefix());
    }

    @Test
    public void getTableReferencesFromShortTableNamesThrowsIfLongNameDoesNotBeginWithPrefix() {
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> nonTableMappingTableNameGetter.getTableReferencesFromShortTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                NON_TABLE_MAPPING_DDL_CONFIG.tablePrefix());
    }

    @Test
    public void getTableReferencesFromShortOverflowTableNamesThrowsIfLongNameDoesNotBeginWithPrefix() {
        assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
                () -> nonTableMappingTableNameGetter.getTableReferencesFromShortOverflowTableNames(
                        connectionSupplier, SHORT_TABLE_NAMES),
                NON_TABLE_MAPPING_DDL_CONFIG.overflowTablePrefix());
    }

    private Set<String> getLongOverflowTableNames() {
        return TABLE_REFERENCES.stream()
                .map(tableMappingTableNameGetter::getPrefixedOverflowTableName)
                .collect(Collectors.toSet());
    }

    private Set<String> getLongTableNames() {
        return TABLE_REFERENCES.stream()
                .map(tableMappingTableNameGetter::getPrefixedTableName)
                .collect(Collectors.toSet());
    }

    private static void assertThatLoggableExceptionThrownByMatchesPrefixMissingException(
            ThrowingCallable callable, String prefix) {
        assertThatLoggableExceptionThrownBy(callable)
                .hasLogMessage("Long table name does not begin with prefix")
                .containsArgs(SafeArg.of("prefix", prefix));
    }
}
