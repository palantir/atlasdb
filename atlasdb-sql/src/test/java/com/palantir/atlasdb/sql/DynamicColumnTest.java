package com.palantir.atlasdb.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TestPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.TestSchema;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class DynamicColumnTest {
    public static final String CONFIG_FILENAME = "memoryTestConfig.yml";

    @Before
    public void setup() throws SQLException, ClassNotFoundException {
        try (Connection c = TestConnection.create(CONFIG_FILENAME)) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();
        fillDynTable(txm, kvs);
    }

    @Test
    public void testSelectDynamic() throws SQLException {
        Statement stmt = null;
        ResultSet results = null;
        try (Connection c = TestConnection.create(CONFIG_FILENAME)) {
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

    @After
    public void teardown() throws SQLException, ClassNotFoundException {
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
        long l = i * 1000;
        String s = "key" + i;
        return EncodingUtils.add(PtBytes.toBytes(l), PtBytes.toBytes(s));
    }

    private byte[] col(int i) {
        return Longs.toByteArray(111L * i);
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

    public void validateResults(ResultSet results, String rowName, String row, String colName, Object expectedValue) throws SQLException {
  /*      Preconditions.checkArgument(results.getString(rowName).equals(row));
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
        }*/
    }

}
