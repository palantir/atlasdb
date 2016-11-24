/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public class TableSizeCacheTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("ns.test_table");
    ConnectionSupplier conns = mock(ConnectionSupplier.class);
    TableSizeCache tableSizeCache = new TableSizeCache(conns, AtlasDbConstants.DEFAULT_METADATA_TABLE);

    @Before
    public void setup() {
        SqlConnection mockConnection =  mock(SqlConnection.class);
        when(conns.getNewUnsharedConnection()).thenReturn(mockConnection);

        AgnosticResultSet resultSet = mock(AgnosticResultSet.class);
        when(mockConnection.selectResultSetUnregisteredQuery(startsWith("SELECT table_size FROM"), anyObject()))
                .thenReturn(resultSet);

        AgnosticResultRow row = mock(AgnosticResultRow.class);
        when(row.getInteger(eq("table_size"))).thenReturn(TableSize.OVERFLOW.getId());
        doReturn(ImmutableList.of(row)).when(resultSet).rows();

        when(mockConnection.getUnderlyingConnection()).thenReturn(mock(Connection.class));
    }

    @Test
    public void testGetTableSizeOneTimeHasCacheMiss() throws Exception {
        assertThat(tableSizeCache.getTableSize(TEST_TABLE), is(TableSize.OVERFLOW));
        verify(conns, times(1)).getNewUnsharedConnection();
    }


    @Test
    public void testGetTableSizeForSameTableHitsCache() throws Exception {
        assertThat(tableSizeCache.getTableSize(TEST_TABLE), is(TableSize.OVERFLOW));
        assertThat(tableSizeCache.getTableSize(TEST_TABLE), is(TableSize.OVERFLOW));

        verify(conns, times(1)).getNewUnsharedConnection();
    }

    @Test
    public void testCacheInvalidationHitsConnectionAgain() throws Exception {
        assertThat(tableSizeCache.getTableSize(TEST_TABLE), is(TableSize.OVERFLOW));
        tableSizeCache.clearCacheForTable(TEST_TABLE);
        assertThat(tableSizeCache.getTableSize(TEST_TABLE), is(TableSize.OVERFLOW));

        verify(conns, times(2)).getNewUnsharedConnection();
    }
}
