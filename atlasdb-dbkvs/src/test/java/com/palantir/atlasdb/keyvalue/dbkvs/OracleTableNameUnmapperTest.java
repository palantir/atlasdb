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
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.AgnosticResultSetImpl;
import com.palantir.nexus.db.sql.SqlConnection;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

public class OracleTableNameUnmapperTest {

    private static final String TEST_PREFIX = "a_";
    private static final Namespace TEST_NAMESPACE = Namespace.create("test_namespace");
    private static final String LONG_TABLE_NAME = "ThisIsAVeryLongTableNameThatWillExceed";
    private static final String SHORT_TABLE_NAME = "testShort";

    private static final TableReference TABLE_REF = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME);
    private static final TableReference TABLE_REF_2 = TableReference.create(TEST_NAMESPACE, LONG_TABLE_NAME + "2");

    private OracleTableNameUnmapper oracleTableNameUnmapper;
    private AgnosticResultSet shortNameResultSet;
    private ConnectionSupplier connectionSupplier;
    private SqlConnection sqlConnection;

    @Before
    public void setup() {
        connectionSupplier = mock(ConnectionSupplier.class);
        oracleTableNameUnmapper = new OracleTableNameUnmapper();
        sqlConnection = mock(SqlConnection.class);
        Connection connection = mock(Connection.class);
        when(sqlConnection.getUnderlyingConnection()).thenReturn(connection);
        when(connectionSupplier.get()).thenReturn(sqlConnection);

        shortNameResultSet = mock(AgnosticResultSet.class);
        when(sqlConnection.selectResultSetUnregisteredQuery(
                        startsWith("SELECT short_table_name FROM atlasdb_table_names WHERE table_name"), any()))
                .thenReturn(shortNameResultSet);
    }

    @Test
    public void shouldThrowIfTableMappingDoesNotExist() throws TableMappingNotFoundException {
        when(shortNameResultSet.size()).thenReturn(0);
        assertThatThrownBy(() -> oracleTableNameUnmapper.getShortTableNameFromMappingTable(
                        connectionSupplier, TEST_PREFIX, TABLE_REF))
                .isInstanceOf(TableMappingNotFoundException.class)
                .hasMessageContaining("The table a_test_namespace__ThisIsAVeryLongTableNameThatWillExceed");
    }

    @Test
    public void shouldReturnIfTableMappingExists() throws TableMappingNotFoundException {
        when(shortNameResultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(SHORT_TABLE_NAME);
        doReturn(ImmutableList.of(row)).when(shortNameResultSet).rows();

        String shortName =
                oracleTableNameUnmapper.getShortTableNameFromMappingTable(connectionSupplier, TEST_PREFIX, TABLE_REF);
        assertThat(shortName).isEqualTo(SHORT_TABLE_NAME);
    }

    @Test
    public void cacheIsActuallyUsed() throws TableMappingNotFoundException {
        // do a normal read
        when(shortNameResultSet.size()).thenReturn(1);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getString(eq("short_table_name"))).thenReturn(SHORT_TABLE_NAME);
        doReturn(ImmutableList.of(row)).when(shortNameResultSet).rows();

        String shortName =
                oracleTableNameUnmapper.getShortTableNameFromMappingTable(connectionSupplier, TEST_PREFIX, TABLE_REF_2);
        assertThat(shortName).isEqualTo(SHORT_TABLE_NAME);

        // verify that underlying datastore was called once instead of hitting the cache
        verify(row, times(1)).getString("short_table_name");

        // read again looking for the same table
        oracleTableNameUnmapper.getShortTableNameFromMappingTable(connectionSupplier, TEST_PREFIX, TABLE_REF_2);

        // verify that cache was hit and underlying datastore was _still_ only called once
        verify(row, times(1)).getString("short_table_name");
    }

    @Test
    public void getLongTableNamesFromMappingTableReturnsLongNames() throws TableMappingNotFoundException {
        ArgumentCaptor<Object> tableNameCaptor = ArgumentCaptor.forClass(Object.class);
        Map<String, String> shortNamesToLongNames = ImmutableMap.<String, String>builder()
                .put("shortNameOne", "superLongNameOne")
                .put("shortNameTwo", "superLongNameTwo")
                .put("shortNameThree", "superLongNameThree")
                .build();
        mockShortNamesToLongNamesQuery(shortNamesToLongNames.values());

        Set<String> longNames = oracleTableNameUnmapper.getLongTableNamesFromMappingTable(
                connectionSupplier, shortNamesToLongNames.keySet());

        assertThat(longNames).containsExactlyInAnyOrderElementsOf(shortNamesToLongNames.values());
        verify(sqlConnection)
                .selectResultSetUnregisteredQuery(
                        eq("SELECT table_name FROM atlasdb_table_names WHERE LOWER(short_table_name) IN ("
                                + generatePlaceholders(3) + ")"),
                        tableNameCaptor.capture());

        assertThat(tableNameCaptor.getAllValues()).containsExactlyInAnyOrderElementsOf(shortNamesToLongNames.keySet());
        verifyNoMoreInteractions(sqlConnection);
    }

    @Test
    public void getLongTableNamesFromMappingTableDoesMultipleQueriesIfMoreTablesThanThreshold()
            throws TableMappingNotFoundException {
        int numberOfEntries = AtlasDbConstants.IN_CLAUSE_EXPRESSION_LIMIT + 100;
        Map<String, String> shortNamesToLongNames = IntStream.range(0, numberOfEntries)
                .boxed()
                .collect(Collectors.toMap(i -> "shortName" + i, i -> "longName" + i));
        mockShortNamesToLongNamesQuery(shortNamesToLongNames.values());

        Set<String> longNames = oracleTableNameUnmapper.getLongTableNamesFromMappingTable(
                connectionSupplier, shortNamesToLongNames.keySet());

        assertThat(longNames).containsExactlyInAnyOrderElementsOf(shortNamesToLongNames.values());
        verify(sqlConnection)
                .selectResultSetUnregisteredQuery(
                        eq("SELECT table_name FROM atlasdb_table_names WHERE LOWER(short_table_name) IN ("
                                + generatePlaceholders(AtlasDbConstants.IN_CLAUSE_EXPRESSION_LIMIT) + ")"),
                        any());
        verify(sqlConnection)
                .selectResultSetUnregisteredQuery(
                        eq("SELECT table_name FROM atlasdb_table_names WHERE LOWER(short_table_name) IN ("
                                + generatePlaceholders(numberOfEntries - AtlasDbConstants.IN_CLAUSE_EXPRESSION_LIMIT)
                                + ")"),
                        any());
        verifyNoMoreInteractions(sqlConnection);
    }

    @Test
    public void getLongTableNamesThrowsIfPartialResultsReturned() {
        mockShortNamesToLongNamesQuery(Set.of("test"));

        assertThatThrownBy(() -> oracleTableNameUnmapper.getLongTableNamesFromMappingTable(
                        connectionSupplier, Set.of("test", "test_2")))
                .isInstanceOf(TableMappingNotFoundException.class);
    }

    private void mockShortNamesToLongNamesQuery(Collection<String> longTableNames) {
        OngoingStubbing<AgnosticResultSet> ongoingStubbing = when(sqlConnection.selectResultSetUnregisteredQuery(
                startsWith("SELECT table_name FROM " + AtlasDbConstants.ORACLE_NAME_MAPPING_TABLE + " WHERE"
                        + " LOWER(short_table_name)"),
                any()));

        for (List<String> batch : Iterables.partition(longTableNames, AtlasDbConstants.IN_CLAUSE_EXPRESSION_LIMIT)) {
            ongoingStubbing = ongoingStubbing.thenReturn(new AgnosticResultSetImpl(
                    batch.stream().map(x -> (Object) x).map(List::of).collect(Collectors.toList()),
                    DBType.ORACLE,
                    Map.of("table_name", 0, "TABLE_NAME", 0)));
        }
    }

    private String generatePlaceholders(int numberOfPlaceholders) {
        return String.join(",", Collections.nCopies(numberOfPlaceholders, "LOWER(?)"));
    }
}
