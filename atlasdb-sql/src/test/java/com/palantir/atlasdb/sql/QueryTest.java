package com.palantir.atlasdb.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static com.palantir.atlasdb.sql.QueryTests.IN_MEMORY_TEST_CONFIG;
import static com.palantir.atlasdb.sql.QueryTests.assertFails;
import static com.palantir.atlasdb.sql.QueryTests.count;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class QueryTest {

    private static final String ROW_COMP1 = "row1";
    private static final String ROW_COMP2 = "row2";
    private static final String COL1_NAME = "col1";
    private static final String COL1_LABEL = "first";
    private static final String COL2_NAME = "col2";

    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "table");

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        try (Connection ignored = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();

        TableDefinition tableDef = new TableDefinition() {{
            rowName();
            rowComponent(ROW_COMP1, ValueType.FIXED_LONG);
            rowComponent(ROW_COMP2, ValueType.STRING);
            columns();
            column(COL1_LABEL, COL1_NAME, ValueType.STRING);
            column(COL2_NAME, COL2_NAME, ValueType.STRING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};
        kvs.createTable(TABLE, tableDef.toTableMetadata().persistToBytes());
        kvs.putMetadataForTable(TABLE, tableDef.toTableMetadata().persistToBytes());
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(TABLE, ImmutableMap.of(
                    Cell.create(row(1), colName(1).getBytes()), "value1".getBytes(),
                    Cell.create(row(1), colName(2).getBytes()), "value3".getBytes(),
                    Cell.create(row(2), colName(1).getBytes()), "value2".getBytes()));
            return null;
        });
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

    private static String colName(int i) {
        return "col" + i;
    }

    @Test
    public void testSelect() {
        testFindsAllData(String.format("select %s,%s from %s", ROW_COMP2, COL1_NAME, TABLE.getQualifiedName()));
    }

    @Test
    public void testSelectAll() {
        testFindsAllData(String.format("select * from %s", TABLE.getQualifiedName()));
    }

    private void testFindsAllData(String sql) {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(sql);
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(1)));
            assertThat(results.getBytes(COL1_NAME), equalTo("value1".getBytes()));
            assertThat(results.getString(COL1_NAME), equalTo("value1"));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(2)));
            assertThat(results.getBytes(COL1_NAME), equalTo("value2".getBytes()));
            assertThat(results.getString(COL1_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectRowComp() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select %s from %s", ROW_COMP2, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(1)));
            assertFails(() -> results.getString(COL1_NAME));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(key(2)));
            assertFails(() -> results.getString(COL1_NAME));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectCol() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select %s from %s", COL1_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_NAME), equalTo("value1"));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMultipleCol() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select %s, %s from %s", COL1_NAME, COL1_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_NAME), equalTo("value1"));  // although this does not disambiguate between identical entries
            results.next();
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getString(COL1_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    /**  NB: Currently, COUNT works only for numeric columns. In general, type switching is not permitted.
     *   Aggregate columns should be aliased separately.
     */
    @Test
    public void testSelectCount() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select count(%s) from %s", ROW_COMP1, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(2L));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMax() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select max(%s) from %s", ROW_COMP1, TABLE
                    .getQualifiedName()));
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(22L));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectMin() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select min(%s) from %s", ROW_COMP1, TABLE
                    .getQualifiedName()));
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(11L));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectWhere() {
        testSelectWhere(key(1), "value1");
        testSelectWhere(key(2), "value2");
    }

    private void testSelectWhere(String row, String val) {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from %s where %s = \"%s\"", TABLE.getQualifiedName(), COL1_NAME, val));
            results.next();
            assertThat(results.getString(ROW_COMP2), equalTo(row));
            assertThat(results.getString(COL1_NAME), equalTo(val));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectEmptyCol() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from %s", TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(COL2_NAME), equalTo("value3"));
            results.next();
            assertThat(results.getBytes(COL2_NAME).length, equalTo(0));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
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
        countRowsFromRowFilteringQuery(key(1), "!=", 1);

        countRowsFromRowFilteringQuery(key(2), "=", 1);
        countRowsFromRowFilteringQuery(key(2), ">=", 1);
        countRowsFromRowFilteringQuery(key(2), "<=", 2);
        countRowsFromRowFilteringQuery(key(2), ">", 0);
        countRowsFromRowFilteringQuery(key(2), "<", 1);
        countRowsFromRowFilteringQuery(key(2), "<>", 1);

        countRowsFromRowFilteringQuery("key", ">", 2);
        countRowsFromRowFilteringQuery("key", "<", 0);
        countRowsFromRowFilteringQuery(key(50), "<", 2);
        countRowsFromRowFilteringQuery(key(50), ">", 0);
    }

    private void countRowsFromRowFilteringQuery(String key, String op, int expectedCount) {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(
                     String.format("select * from %s where %s %s \"%s\"", TABLE.getQualifiedName(), ROW_COMP2, op, key));
             assertThat(count(results), equalTo(expectedCount));
         } catch (ClassNotFoundException | SQLException e) {
             throw new RuntimeException("Failure running select.", e);
         }
    }

}
