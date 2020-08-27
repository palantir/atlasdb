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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.sql.AgnosticResultSetImpl;
import com.palantir.nexus.db.sql.SqlConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PostgresDdlTableTest {
    private PostgresDdlTable postgresDdlTable;
    private static final ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test");

    private static final long NOW_MILLIS = 1000;
    private static final long COMPACT_INTERVAL_MILLIS = 100;
    private static final long SMALL_POSITIVE_FACTOR = 10;

    @Before
    public void setUp() {
        postgresDdlTable = new PostgresDdlTable(TEST_TABLE,
                connectionSupplier,
                ImmutablePostgresDdlConfig.builder()
                        .compactInterval(HumanReadableDuration.milliseconds(COMPACT_INTERVAL_MILLIS))
                        .build());
    }

    @Test
    public void shouldCompactIfVacuumWasNeverPerformedAndTheDbTimeIsLessThanCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(null, COMPACT_INTERVAL_MILLIS / SMALL_POSITIVE_FACTOR);

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfVacuumWasNeverPerformedAndTheDbTimeIsMoreThanCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(null, COMPACT_INTERVAL_MILLIS * SMALL_POSITIVE_FACTOR);

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfVacuumWasPerformedExactlyBeforeCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS - COMPACT_INTERVAL_MILLIS, NOW_MILLIS);

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfVacuumWasPerformedBeforeCompactInterval() throws Exception {
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS - COMPACT_INTERVAL_MILLIS * SMALL_POSITIVE_FACTOR,
                NOW_MILLIS);

        assertThatVacuumWasPerformed(sqlConnection);
    }

    @Test
    public void shouldNotCompactIfVacuumWasPerformedWithinCompactInterval() {
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS - COMPACT_INTERVAL_MILLIS / SMALL_POSITIVE_FACTOR,
                NOW_MILLIS);
        assertThatVacuumWasNotPerformed(sqlConnection);
    }

    @Test
    public void shouldNotCompactIfVacuumTimestampExceedsNowTimestampByLessThanCompactInterval() {
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS + COMPACT_INTERVAL_MILLIS / SMALL_POSITIVE_FACTOR,
                NOW_MILLIS);
        assertThatVacuumWasNotPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfVacuumTimestampExceedsNowTimestampByMoreThanCompactInterval() {
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS + COMPACT_INTERVAL_MILLIS * SMALL_POSITIVE_FACTOR,
                NOW_MILLIS);
        assertThatVacuumWasNotPerformed(sqlConnection);
    }

    @Test
    public void shouldCompactIfCompactMillisIsSetToZero() throws Exception {
        postgresDdlTable = new PostgresDdlTable(TEST_TABLE,
                connectionSupplier,
                ImmutablePostgresDdlConfig.builder().compactInterval(HumanReadableDuration.valueOf("0 ms")).build());
        SqlConnection sqlConnection = setUpSqlConnection(NOW_MILLIS - SMALL_POSITIVE_FACTOR, NOW_MILLIS);
        assertThatVacuumWasPerformed(sqlConnection, false);

        verify(sqlConnection, never()).selectResultSetUnregisteredQuery(startsWith("SELECT FLOOR"), any());
    }

    private SqlConnection setUpSqlConnection(Long lastVacuumTimestamp, Long currentTimestamp) {
        SqlConnection sqlConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(sqlConnection);

        List<List<Object>> selectResults = new ArrayList<>();
        selectResults.add(Arrays.asList(new Object[] {lastVacuumTimestamp, currentTimestamp}));

        Mockito.when(sqlConnection.selectResultSetUnregisteredQuery(startsWith("SELECT FLOOR"), any()))
                .thenReturn(
                        new AgnosticResultSetImpl(selectResults,
                                DBType.POSTGRESQL,
                                // This is the columnName to position mapping in the results map. Column Names are both
                                // upper-case and lower-case, as postgres allows the query to have either of them.
                                new ImmutableMap.Builder<String, Integer>()
                                        .put("last", 0)
                                        .put("LAST", 0)
                                        .put("current", 1)
                                        .put("CURRENT", 1)
                                        .build()));
        return sqlConnection;
    }

    private void assertThatVacuumWasPerformed(SqlConnection sqlConnection) {
        assertThatVacuumWasPerformed(sqlConnection, true);
    }

    private void assertThatVacuumWasPerformed(SqlConnection sqlConnection, boolean assertThatTimestampsWereChecked) {
        assertTrue(postgresDdlTable.shouldRunCompaction());
        postgresDdlTable.compactInternally(false);

        if (assertThatTimestampsWereChecked) {
            verify(sqlConnection, times(2)).selectResultSetUnregisteredQuery(startsWith("SELECT FLOOR"), any());
        }

        verify(sqlConnection).executeUnregisteredQuery(eq("VACUUM ANALYZE " + DbKvs.internalTableName(TEST_TABLE)));
    }

    private void assertThatVacuumWasNotPerformed(SqlConnection sqlConnection) {
        assertFalse(postgresDdlTable.shouldRunCompaction());
        postgresDdlTable.compactInternally(false);

        verify(sqlConnection, times(2)).selectResultSetUnregisteredQuery(startsWith("SELECT FLOOR"), any());

        verify(sqlConnection, never()).executeUnregisteredQuery(
                eq("VACUUM ANALYZE " + DbKvs.internalTableName(TEST_TABLE)));
    }
}
