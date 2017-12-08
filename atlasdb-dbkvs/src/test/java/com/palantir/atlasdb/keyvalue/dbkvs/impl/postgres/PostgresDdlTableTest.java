/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSetImpl;
import com.palantir.nexus.db.sql.SqlConnection;

public class PostgresDdlTableTest {
    private PostgresDdlTable postgresDdlTable;
    private static final ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test");
    private static final int NOW_MILLIS = 100;

    @Before
    public void setUp() {
        postgresDdlTable = new PostgresDdlTable(TEST_TABLE,
                connectionSupplier,
                ImmutablePostgresDdlConfig.builder().compactIntervalMillis(10).build());
    }

    @Test
    public void shouldCompactIfVacuumWasNeverPerformed() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(null, null, null, null);
        assertTrue(postgresDdlTable.checkIfTableHasNotBeenCompactedForCompactIntervalMillis());

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfVacuumWasPerformedBeforeCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(new Timestamp(10), null, new Timestamp(9), null);
        assertTrue(postgresDdlTable.checkIfTableHasNotBeenCompactedForCompactIntervalMillis());

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfAutoVacuumWasPerformedBeforeCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(null, new Timestamp(10), null, new Timestamp(9));
        assertTrue(postgresDdlTable.checkIfTableHasNotBeenCompactedForCompactIntervalMillis());

        assertThatVacuumWasPerformed(sqlConnection);
    }


    @Test
    public void shouldNotCompactIfAutoVacuumWasPerformedWithinCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(null, new Timestamp(95), null, new Timestamp(92));
        assertThatVacuumWasNotPerformed(sqlConnection);
    }


    @Test
    public void shouldNotCompactIfVacuumWasPerformedWithinCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(new Timestamp(95), null, new Timestamp(92), null);
        assertThatVacuumWasNotPerformed(sqlConnection);
    }

    @Test
    public void shouldNotCompactIfVacuumDidOccurInLastCompactMillis() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(new Timestamp(5), null, new Timestamp(90), new Timestamp(10));

        assertThatVacuumWasNotPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfCompactMillisIsSetToZero() throws Exception {
        postgresDdlTable = new PostgresDdlTable(TEST_TABLE,
                connectionSupplier,
                ImmutablePostgresDdlConfig.builder().compactIntervalMillis(0).build());
        SqlConnection sqlConnection = setUpSqlConnection(new Timestamp(5), null, new Timestamp(90), new Timestamp(10));
        assertThatVacuumWasPerformed(sqlConnection);
        verify(sqlConnection, never()).selectResultSetUnregisteredQuery(eq("SELECT CURRENT_TIMESTAMP"));
        verify(sqlConnection, never()).selectResultSetUnregisteredQuery(startsWith("SELECT relname"), any());
    }

    private SqlConnection setUpSqlConnection(Timestamp lastVacuumTimestamp, Timestamp lastAutoVacuumTimestamp,
            Timestamp lastAnalyzeTimestamp, Timestamp lastAutoAnalyzeTimestamp) {
        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);

        List<List<Object>> selectResults = new ArrayList<>();
        selectResults.add(Arrays.asList(
                new Object[] {"ns__test", lastVacuumTimestamp, lastAutoVacuumTimestamp, lastAnalyzeTimestamp,
                              lastAutoAnalyzeTimestamp}));

        List<List<Object>> timestampList = new ArrayList<>();
        timestampList.add(Arrays.asList(new Object[] {new Timestamp(NOW_MILLIS)}));

        when(sqlConnection.selectResultSetUnregisteredQuery(eq("SELECT CURRENT_TIMESTAMP")))
                .thenReturn(new AgnosticResultSetImpl(timestampList,
                        DBType.POSTGRESQL,
                        new ImmutableMap.Builder<String, Integer>()
                                .put("now", 0)
                                .put("NOW", 0)
                                .build()));

        Mockito.when(sqlConnection.selectResultSetUnregisteredQuery(startsWith("SELECT relname"), any()))
                .thenReturn(
                        new AgnosticResultSetImpl(selectResults,
                                DBType.POSTGRESQL,
                                // This is the columnName to position in the results map. Column Names are both uppercase
                                // and lowercase, as postgres allows the query to have either of them.
                                new ImmutableMap.Builder<String, Integer>()
                                        .put("relname", 0)
                                        .put("RELNAME", 0)
                                        .put("LAST_AUTOVACUUM", 2)
                                        .put("last_autovacuum", 2)
                                        .put("last_analyze", 3)
                                        .put("LAST_ANALYZE", 3)
                                        .put("last_vacuum", 1)
                                        .put("LAST_VACUUM", 1)
                                        .put("last_autoanalyze", 4)
                                        .put("LAST_AUTOANALYZE", 4)
                                        .build()));
        return sqlConnection;
    }

    private void assertThatVacuumWasPerformed(SqlConnection sqlConnection) {
        postgresDdlTable.compactInternally();

        verify(sqlConnection).executeUnregisteredQuery(eq("VACUUM ANALYZE " + DbKvs.internalTableName(TEST_TABLE)));
    }

    private void assertThatVacuumWasNotPerformed(SqlConnection sqlConnection) {
        assertFalse(postgresDdlTable.checkIfTableHasNotBeenCompactedForCompactIntervalMillis());
        postgresDdlTable.compactInternally();

        verify(sqlConnection, times(2)).selectResultSetUnregisteredQuery(eq("SELECT CURRENT_TIMESTAMP"));
        verify(sqlConnection, times(2)).selectResultSetUnregisteredQuery(startsWith("SELECT relname"), any());

        verify(sqlConnection, never()).executeUnregisteredQuery(
                eq("VACUUM ANALYZE " + DbKvs.internalTableName(TEST_TABLE)));
    }
}
