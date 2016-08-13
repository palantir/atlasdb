package com.palantir.atlasdb.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static com.palantir.atlasdb.sql.QueryTests.IN_MEMORY_TEST_CONFIG;
import static com.palantir.atlasdb.sql.QueryTests.count;
import static com.palantir.atlasdb.sql.QueryTests.assertFails;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class QueryTest {

    private static final String ROW_COMP = "row";
    private static final String COL1_NAME = "col1";
    private static final byte[] COL1_IN_BYTES = COL1_NAME.getBytes();
    private static final String COL1_LABEL = "first";
    private static final String COL2_NAME = "col2";
    private static final byte[] COL2_IN_BYTES = COL2_NAME.getBytes();

    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "table");

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        try (Connection c = getConnection()) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();

        TableDefinition tableDef = new TableDefinition() {{
            rowName();
            rowComponent(ROW_COMP, ValueType.STRING);
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
                    Cell.create("key1".getBytes(), COL1_IN_BYTES), "value1".getBytes(),
                    Cell.create("key1".getBytes(), COL2_IN_BYTES), "value3".getBytes(),
                    Cell.create("key2".getBytes(), COL1_IN_BYTES), "value2".getBytes()));
            return null;
        });
    }

    @After
    public void teardown() throws SQLException, ClassNotFoundException {
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        KeyValueService kvs = services.getKeyValueService();
        kvs.dropTable(TABLE);
    }

    @Test
    public void testSelect() {
        testFindsAllData(String.format("select %s,%s from %s", ROW_COMP, COL1_NAME, TABLE.getQualifiedName()));
    }

    @Test
    public void testSelectAll() {
        testFindsAllData(String.format("select * from %s", TABLE.getQualifiedName()));
    }

    private void testFindsAllData(String sql) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(sql);
            results.next();
            assertThat(results.getString(ROW_COMP), equalTo("key1"));
            assertThat(results.getBytes(COL1_NAME), equalTo("value1".getBytes()));
            assertThat(results.getString(COL1_NAME), equalTo("value1"));
            results.next();
            assertThat(results.getString(ROW_COMP), equalTo("key2"));
            assertThat(results.getBytes(COL1_NAME), equalTo("value2".getBytes()));
            assertThat(results.getString(COL1_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectRowComp() {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select %s from %s", ROW_COMP, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(ROW_COMP), equalTo("key1"));
            assertFails(() -> results.getString(COL1_NAME));
            results.next();
            assertThat(results.getString(ROW_COMP), equalTo("key2"));
            assertFails(() -> results.getString(COL1_NAME));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectCol() {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select %s from %s", COL1_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getString(ROW_COMP));
            assertThat(results.getString(COL1_NAME), equalTo("value1"));
            results.next();
            assertFails(() -> results.getString(ROW_COMP));
            assertThat(results.getString(COL1_NAME), equalTo("value2"));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectWhere() {
        testSelectWhere("key1", "value1");
        testSelectWhere("key2", "value2");
    }

    private void testSelectWhere(String row, String val) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from %s where %s = \"%s\"", TABLE.getQualifiedName(), COL1_NAME, val));
            results.next();
            assertThat(results.getString(ROW_COMP), equalTo(row));
            assertThat(results.getString(COL1_NAME), equalTo(val));
            assertThat(results.next(), equalTo(false));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectEmptyCol() {
        try (Connection c = getConnection()) {
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
        countRowsFromRowFilteringQuery("key1", "=", 1);
        countRowsFromRowFilteringQuery("key1", ">=", 2);
        countRowsFromRowFilteringQuery("key1", "<=", 1);
        countRowsFromRowFilteringQuery("key1", ">", 1);
        countRowsFromRowFilteringQuery("key1", "<", 0);
        countRowsFromRowFilteringQuery("key1", "!=", 1);

        countRowsFromRowFilteringQuery("key2", "=", 1);
        countRowsFromRowFilteringQuery("key2", ">=", 1);
        countRowsFromRowFilteringQuery("key2", "<=", 2);
        countRowsFromRowFilteringQuery("key2", ">", 0);
        countRowsFromRowFilteringQuery("key2", "<", 1);
        countRowsFromRowFilteringQuery("key2", "<>", 1);

        countRowsFromRowFilteringQuery("key", ">", 0);
        countRowsFromRowFilteringQuery("key", "<", 0);
        countRowsFromRowFilteringQuery("key50", "<", 2);
        countRowsFromRowFilteringQuery("key50", ">", 0);
    }

    private void countRowsFromRowFilteringQuery(String key, String op, int expectedCount) {
         try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select * from %s where %s %s \"%s\"", TABLE.getQualifiedName(), ROW_COMP, op, key));
            assertThat(count(results), equalTo(expectedCount));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(AtlasJdbcDriver.class.getName());
        final String configFilePath = ConnectionTest.class.getClassLoader().getResource(IN_MEMORY_TEST_CONFIG).getFile();
        final String uri = "jdbc:atlas?configFile=" + configFilePath;
        return DriverManager.getConnection(uri);
    }

}
