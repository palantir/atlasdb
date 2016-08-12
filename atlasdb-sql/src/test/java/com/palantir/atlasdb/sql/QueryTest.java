package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
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
    private static final String COL_NAME = "col";
    private static final String COL_LABEL = "col_name";
    private static final byte[] COLUMN_NAME_IN_BYTES = COL_NAME.getBytes();
    private static final TableReference tableRef = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test_table");
    public static final String CONFIG_FILENAME = "memoryTestConfig.yml";

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
            column(COL_LABEL, COL_NAME, ValueType.STRING);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};
        kvs.createTable(tableRef, tableDef.toTableMetadata().persistToBytes());
        kvs.putMetadataForTable(tableRef, tableDef.toTableMetadata().persistToBytes());
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(tableRef, ImmutableMap.of(Cell.create("key1".getBytes(), COLUMN_NAME_IN_BYTES), "value1".getBytes()));
            t.put(tableRef, ImmutableMap.of(Cell.create("key2".getBytes(), COLUMN_NAME_IN_BYTES), "value2".getBytes()));
            return null;
        });
    }

    @After
    public void teardown() throws SQLException, ClassNotFoundException {
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        KeyValueService kvs = services.getKeyValueService();
        kvs.truncateTable(tableRef);
    }

    @Test
    public void testSelect() {
        testFindsAllData(String.format("select row,col from %s", tableRef.getQualifiedName()));
    }

    @Test
    public void testSelectAll() {
        testFindsAllData(String.format("select * from %s", tableRef.getQualifiedName()));
    }

    private void testFindsAllData(String sql) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(sql);
            results.next();
            Preconditions.checkArgument(results.getString(ROW_COMP).equals("key1"));
            Preconditions.checkArgument(Arrays.equals(results.getBytes(COL_NAME), "value1".getBytes()));
            Preconditions.checkArgument(results.getString(COL_NAME).equals("value1"));
            results.next();
            Preconditions.checkArgument(results.getString(ROW_COMP).equals("key2"));
            Preconditions.checkArgument(Arrays.equals(results.getBytes(COL_NAME), "value2".getBytes()));
            Preconditions.checkArgument(results.getString(COL_NAME).equals("value2"));
            Preconditions.checkArgument(!results.next());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectRowComp() {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select row from %s", tableRef.getQualifiedName()));
            results.next();
            Preconditions.checkArgument(results.getString(ROW_COMP).equals("key1"));
            Preconditions.checkArgument(fails(() -> results.getString(COL_NAME).equals("value1")));
            results.next();
            Preconditions.checkArgument(results.getString(ROW_COMP).equals("key2"));
            Preconditions.checkArgument(fails(() -> results.getString(COL_NAME).equals("value2")));
            Preconditions.checkArgument(!results.next());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectCol() {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select col from %s", tableRef.getQualifiedName()));
            results.next();
            Preconditions.checkArgument(fails(() -> results.getString(ROW_COMP).equals("key1")));
            Preconditions.checkArgument(results.getString(COL_NAME).equals("value1"));
            results.next();
            Preconditions.checkArgument(fails(() -> results.getString(ROW_COMP).equals("key2")));
            Preconditions.checkArgument(results.getString(COL_NAME).equals("value2"));
            Preconditions.checkArgument(!results.next());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @Test
    public void testSelectWhere1() {
        testSelectWhere("key1", "value1");
    }

    @Test
    public void testSelectWhere2() {
        testSelectWhere("key2", "value2");
    }

    private void testSelectWhere(String row, String val) {
        try (Connection c = getConnection()) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from %s where %s = %s", tableRef.getQualifiedName(), COL_NAME, val));
            results.next();
            Preconditions.checkArgument(results.getString(ROW_COMP).equals(row));
            Preconditions.checkArgument(results.getString(COL_NAME).equals(val));
            Preconditions.checkArgument(!results.next());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    private boolean fails(Callable<?> c) {
        try {
            c.call();
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(AtlasJdbcDriver.class.getName());
        final String configFilePath = ConnectionTest.class.getClassLoader().getResource(CONFIG_FILENAME).getFile();
        final String uri = "jdbc:atlas?configFile=" + configFilePath;
        return DriverManager.getConnection(uri);
    }

}
