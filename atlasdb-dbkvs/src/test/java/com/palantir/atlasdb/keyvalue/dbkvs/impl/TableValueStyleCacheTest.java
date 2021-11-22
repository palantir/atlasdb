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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;
import java.sql.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableValueStyleCacheTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test_table");
    private static final TableReference TEST_TABLE_2 = TableReference.createFromFullyQualifiedName("ns.test_table_2");
    private final ConnectionSupplier connectionSupplier = mock(ConnectionSupplier.class);
    private final TableValueStyleCache valueStyleCache = new TableValueStyleCache();

    @Before
    public void setup() {
        SqlConnection mockConnection = mock(SqlConnection.class);
        when(connectionSupplier.get()).thenReturn(mockConnection);

        AgnosticResultSet resultSet = mock(AgnosticResultSet.class);
        when(mockConnection.selectResultSetUnregisteredQuery(startsWith("SELECT table_size FROM"), any()))
                .thenReturn(resultSet);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getInteger(eq("table_size"))).thenReturn(TableValueStyle.OVERFLOW.getId());
        doReturn(ImmutableList.of(row)).when(resultSet).rows();

        when(mockConnection.getUnderlyingConnection()).thenReturn(mock(Connection.class));
    }

    @After
    public void tearDown() {
        valueStyleCache.clearCacheForTable(TEST_TABLE);
        valueStyleCache.clearCacheForTable(TEST_TABLE_2);
    }

    @Test
    public void testGetTableSizeOneTimeHasCacheMiss() throws Exception {
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        verify(connectionSupplier, times(1)).get();
    }

    @Test
    public void testGetTableSizeForSameTableHitsCache() throws Exception {
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);

        verify(connectionSupplier, times(1)).get();
    }

    @Test
    public void testCacheInvalidationHitsConnectionAgain() throws Exception {
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        valueStyleCache.clearCacheForTable(TEST_TABLE);
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);

        verify(connectionSupplier, times(2)).get();
    }

    @Test
    public void testCacheHandlesMultipleTableRequests() throws Exception {
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE_2, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);

        verify(connectionSupplier, times(2)).get();
    }

    @Test
    public void testCacheInvalidatesOnlyOneTable() throws Exception {
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE_2, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        valueStyleCache.clearCacheForTable(TEST_TABLE);

        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        verify(connectionSupplier, times(3)).get();

        // No additional fetch required
        assertThat(valueStyleCache.getTableType(
                        connectionSupplier, TEST_TABLE_2, AtlasDbConstants.DEFAULT_METADATA_TABLE))
                .isEqualTo(TableValueStyle.OVERFLOW);
        verify(connectionSupplier, times(3)).get();
    }
}
