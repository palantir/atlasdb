package com.palantir.atlasdb.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TestPersistence;
import com.palantir.atlasdb.schema.TestSchema;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class ProtobufQueryTest {

    public static final String TEST_OBJECT_JSON = "{\"id\": 11,\"type\": 543,\"is_group\": false,\"deleted\": 0,\"data_event_id\": 100}";

    public static final String COL_NAME = "b";
    public static final String COL_LABEL = "base_object";
    public static final String ROW_NAME = "object_id";
    public static final String CONFIG_FILENAME = "memoryTestConfig.yml";

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        try (Connection c = getConnection(CONFIG_FILENAME)) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();

        fillSimpleTable(txm, kvs);
        fillDynTable(txm, kvs);
    }

    private void fillSimpleTable(TransactionManager txm, KeyValueService kvs) {
        final TableReference tableRef = TestSchema.ONLY_TABLE;
        final TableDefinition tableDef = TestSchema.INSTANCE.getLatestSchema().getTableDefinition(tableRef);
        final TableMetadata tableMetadata = tableDef.toTableMetadata();
        kvs.createTable(tableRef, tableMetadata.persistToBytes());
        kvs.putMetadataForTable(tableRef, tableMetadata.persistToBytes());
        kvs.truncateTable(tableRef);
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(tableRef, ImmutableMap.of(Cell.create(key(1), COL_NAME.getBytes()), obj(1).toByteArray()));
            return null;
        });
    }

    /** Dummy table with dynamic columns
     */
    private void fillDynTable(TransactionManager txm, KeyValueService kvs) {
        final TableReference tableRef = TestSchema.DYNAMIC_COLUMN_TABLE;
        final TableDefinition tableDef = TestSchema.INSTANCE.getLatestSchema().getTableDefinition(tableRef);
        final TableMetadata tableMetadata = tableDef.toTableMetadata();
        kvs.createTable(tableRef, tableMetadata.persistToBytes());
        kvs.putMetadataForTable(tableRef, tableMetadata.persistToBytes());
        kvs.truncateTable(tableRef);
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(tableRef, ImmutableMap.of(Cell.create(key(1), col(1)), obj(1).toByteArray(),
                                            Cell.create(key(1), col(2)), obj(2).toByteArray()));
            t.put(tableRef, ImmutableMap.of(Cell.create(key(2), col(3)), obj(3).toByteArray()));
            return null;
        });
    }

    private byte[] key(int i) {
        return ("key" + i).getBytes();
    }

    private byte[] col(int i) {
        return ("col" + i).getBytes();
    }

    private TestPersistence.TestObject obj(int i) {
        return TestPersistence.TestObject.newBuilder()
                .setId(10L + i)
                .setDeleted(0L)
                .setDataEventId(100L * i)
                .setType(543L)
                .setIsGroup(false)
                .build();
    }


    @After
    public void teardown() throws SQLException, ClassNotFoundException {
    }

    public void validateResults(ResultSet results, String rowName, String row, String colName, Object expectedValue) throws SQLException {
        Preconditions.checkArgument(results.getString(rowName).equals(row));
        if (expectedValue != null) {
            if (expectedValue instanceof String) {
                Preconditions.checkArgument(Arrays.equals(results.getBytes(colName), ((String)expectedValue).getBytes()));
                Preconditions.checkArgument(results.getString(colName).equals(expectedValue));
            } else if (expectedValue instanceof Long) {
                Preconditions.checkArgument(Arrays.equals(results.getBytes(colName), ValueType.VAR_LONG.convertFromJava(expectedValue)));
                Preconditions.checkArgument(results.getLong(colName) == (Long) expectedValue);
            } else if (expectedValue instanceof TestPersistence.TestObject) {
                assertThat(expectedValue, equalTo(results.getObject(COL_NAME)));
            }
        } else {
            Preconditions.checkArgument(results.getBytes(colName) == null);
        }
    }

    @Test
    public void testSelectNamed() throws SQLException {
        Statement stmt = null;
        ResultSet results = null;
        try (Connection c = getConnection(CONFIG_FILENAME)) {
            stmt = c.createStatement();
            results = stmt.executeQuery(String.format("select * from %s", TestSchema.ONLY_TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getString(COL_NAME), equalTo(TEST_OBJECT_JSON));
            validateResults(results, ROW_NAME, "key1", COL_NAME, obj(1));
            Preconditions.checkArgument(!results.next());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        } finally {
            if (results != null) {
                results.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Test
    public void testSelectDynamic() throws SQLException {
        Statement stmt = null;
        ResultSet results = null;
        try (Connection c = getConnection(CONFIG_FILENAME)) {
            stmt = c.createStatement();
            results = stmt.executeQuery(String.format("select * from %s", TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()));
            results.next();
            System.out.println(results);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        } finally {
            if (results != null) {
                results.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private static Connection getConnection(String configFilename) throws ClassNotFoundException, SQLException {
        Class.forName(AtlasJdbcDriver.class.getName());
        final String configFilePath = ConnectionTest.class.getClassLoader().getResource(configFilename).getFile();
        final String uri = "jdbc:atlas?configFile=" + configFilePath;
        return DriverManager.getConnection(uri);
    }

}

