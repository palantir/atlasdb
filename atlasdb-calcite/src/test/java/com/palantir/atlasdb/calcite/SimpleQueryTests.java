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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class SimpleQueryTests {
    private static final String ROW_COMP1 = "row1";
    private static final String ROW_COMP2 = "row2";
    private static final String COL1_DISPLAY_NAME = "first";
    private static final String COL2_DISPLAY_NAME = "col2";
    private static final String COL1_NAME = "col1";
    private static final String COL2_NAME = COL2_DISPLAY_NAME;
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "table");

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        cleanup();
        AtlasDbServices services = AtlasJdbcTestSuite.getAtlasDbServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();

        TableDefinition tableDef = new TableDefinition() {{
            rowName();
            rowComponent(ROW_COMP1, ValueType.FIXED_LONG);
            rowComponent(ROW_COMP2, ValueType.STRING);
            columns();
            column(COL1_DISPLAY_NAME, COL1_NAME, ValueType.STRING);
            column(COL2_DISPLAY_NAME, COL2_NAME, ValueType.STRING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};
        kvs.createTable(TABLE, tableDef.toTableMetadata().persistToBytes());
        kvs.putMetadataForTable(TABLE, tableDef.toTableMetadata().persistToBytes());
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(TABLE, ImmutableMap.of(
                    Cell.create(row(1), COL1_NAME.getBytes()), "value1".getBytes(),
                    Cell.create(row(1), COL2_NAME.getBytes()), "value3".getBytes(),
                    Cell.create(row(2), COL1_NAME.getBytes()), "value2".getBytes()));
            return null;
        });
    }

    @AfterClass
    public static void cleanup() {
        AtlasJdbcTestSuite
                .getAtlasDbServices()
                .getKeyValueService()
                .dropTable(TABLE);
    }

    private String key(int i) {
        return "key" + i;
    }

    private static byte[] row(int i) {
        return EncodingUtils.add(PtBytes.toBytes(Long.MIN_VALUE ^ rowComp1(i)), PtBytes.toBytes(rowComp2(i)));
    }

    private static long rowComp1(int i) {
        return 11L * i;
    }

    private static String rowComp2(int i) {
        return "key" + i;
    }

    @Test
    public void testSelect() {
        testFindsAllData(String.format("select %s,%s from \"%s\"", ROW_COMP2, COL1_DISPLAY_NAME, TABLE.getQualifiedName()));
    }

    @Test
    public void testSelectAll() {
        testFindsAllData(String.format("select * from \"%s\"", TABLE.getQualifiedName()));
    }

    private void testFindsAllData(String sql) {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(sql);
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(1)));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value1"));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(2)));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectRowComp() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s from \"%s\"", ROW_COMP2, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(1)));
            assertFails(() -> results.getString(COL1_DISPLAY_NAME));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(2)));
            assertFails(() -> results.getString(COL1_DISPLAY_NAME));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectCol() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s from \"%s\"", COL1_DISPLAY_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value1"));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMultipleCol() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s, %s from \"%s\"",
                            COL1_DISPLAY_NAME, COL1_DISPLAY_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value1"));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectCount() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select count(*) as \"count\" from \"%s\"", TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getLong("count"), equalTo(2L));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMax() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select max(%s) as \"max\" from \"%s\"", ROW_COMP1, TABLE
                            .getQualifiedName()));
            results.next();
            assertThat(results.getLong("max"), equalTo(22L));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMin() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select min(%s) as \"min\" from \"%s\"", ROW_COMP1, TABLE
                            .getQualifiedName()));
            results.next();
            assertThat(results.getLong("min"), equalTo(11L));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectWhere() {
        testSelectWhere(key(1), "value1");
        testSelectWhere(key(2), "value2");
    }

    private void testSelectWhere(String row, String val) {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select * from \"%s\" where %s = '%s'",
                            TABLE.getQualifiedName(), COL1_DISPLAY_NAME, val));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(row));
            assertThat(results.getString(COL1_DISPLAY_NAME), equalTo(val));
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectEmptyCol() {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from \"%s\"", TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(COL2_DISPLAY_NAME), equalTo("value3"));
            results.next();
            assertThat(results.getString(COL2_DISPLAY_NAME), isEmptyOrNullString());
            assertThat(results.next(), equalTo(false));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectWhereRowFiltering() {
        countRowsFromRowFilteringQuery(key(1), "=", 1);
        countRowsFromRowFilteringQuery(key(1), ">=", 2);
        countRowsFromRowFilteringQuery(key(1), "<=", 1);
        countRowsFromRowFilteringQuery(key(1), ">", 1);
        countRowsFromRowFilteringQuery(key(1), "<", 0);
        countRowsFromRowFilteringQuery(key(1), "<>", 1);

        countRowsFromRowFilteringQuery(key(2), "=", 1);
        countRowsFromRowFilteringQuery(key(2), ">=", 1);
        countRowsFromRowFilteringQuery(key(2), "<=", 2);
        countRowsFromRowFilteringQuery(key(2), ">", 0);
        countRowsFromRowFilteringQuery(key(2), "<", 1);
        countRowsFromRowFilteringQuery(key(2), "<>", 1);

        countRowsFromRowFilteringQuery(key(0), ">", 2);
        countRowsFromRowFilteringQuery(key(0), "<", 0);
        countRowsFromRowFilteringQuery(key(50), "<", 2);
        countRowsFromRowFilteringQuery(key(50), ">", 0);
    }

    private void countRowsFromRowFilteringQuery(String key, String op, int expectedCount) {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select * from \"%s\" where %s %s '%s'",
                            TABLE.getQualifiedName(), ROW_COMP2, op, key));
            assertThat(count(results), equalTo(expectedCount));
        } catch (SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    public static int count(ResultSet results) throws SQLException {
        int i = 0;
        while (results.next()) {
            i++;
        }
        return i;
    }

    public static void assertFails(Callable<?> c) {
        try {
            c.call();
        } catch (Exception e) {
            return; // success
        }
        throw new RuntimeException("the call did not fail");
    }
}
