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
package com.palantir.atlasdb.calcite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class SmokeTests {
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "newtable");
    private static final String ROW_COMP = "comp";
    private static final String COL_NAME = "col";

    @Before
    public void setup() {
        cleanup();
        TableDefinition tableDef = new TableDefinition() {{
            rowName();
            rowComponent(ROW_COMP, ValueType.FIXED_LONG);
            columns();
            column(COL_NAME, COL_NAME, ValueType.STRING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};
        AtlasJdbcTestSuite
                .getAtlasDbServices()
                .getKeyValueService()
                .createTable(TABLE, tableDef.toTableMetadata().persistToBytes());
    }

    @After
    public void cleanup() {
        AtlasJdbcTestSuite
                .getAtlasDbServices()
                .getKeyValueService()
                .dropTable(TABLE);
    }

    @Test
    public void canConnectWithJdbcDriver() {
        AtlasJdbcTestSuite.connect();
    }

    @Test
    public void canFindsTables() throws SQLException {
        List<String> allTableNames = Lists.newArrayList();
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                allTableNames.add(rs.getString(3));
            }
        }
        assertThat(allTableNames, hasItems(TABLE.getQualifiedName()));
    }

    @Test
    public void canFindColumnNames() throws SQLException {
        List<String> allColumnNames = Lists.newArrayList();
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            ResultSet rs = conn.createStatement().executeQuery(
                    String.format("select * from \"%s\"", TABLE.getQualifiedName()));
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                allColumnNames.add(rsmd.getColumnName(i));
            }
        }
        assertThat(allColumnNames, contains(ROW_COMP, COL_NAME));
    }
}
