package com.palantir.atlasdb.sql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import static com.palantir.atlasdb.sql.QueryTest.fails;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;

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
    private static final String ROW_COMP1 = "rowComp1";  //long
    private static final String ROW_COMP2 = "rowComp2";  //string
    private static final String COL_COMP1 = "colComp1";  //long
    private static final String COL_COMP2 = "colComp2";  //string
    public static final String DYN = "dyn";

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
    public void testSelectDynamicAll() throws SQLException, ClassNotFoundException {
        try (Connection c = TestConnection.create(CONFIG_FILENAME);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select * from %s", TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()))) {
            results.next();
            validateResult(results, 1, 1);
            results.next();
            validateResult(results, 1, 2);
            results.next();
            validateResult(results, 2, 3);
            assertFalse(results.next());
        }
    }

    @Test
    public void testSelectDynamicRowComponent() throws SQLException, ClassNotFoundException {
        try (Connection c = TestConnection.create(CONFIG_FILENAME);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s from %s", ROW_COMP2,
                                                                 TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()))) {
            results.next();
            fails(() -> results.getLong(ROW_COMP1));
            assertThat(results.getString(ROW_COMP2), equalTo(rowComp2(1)));
            fails(() -> results.getLong(COL_COMP1));
            fails(() -> results.getString(COL_COMP2));
            assertThat(results.getObject(DYN), equalTo(obj(1))); // ??
        }
    }

    @Test
    public void testSelectDynamicColComponent() throws SQLException, ClassNotFoundException {
        try (Connection c = TestConnection.create(CONFIG_FILENAME);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s from %s", COL_COMP2,
                                                                 TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()))) {
            results.next();
            fails(() -> results.getLong(ROW_COMP1));
            fails(() -> results.getString(ROW_COMP2));
            fails(() -> results.getLong(COL_COMP1));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(1)));
            assertThat(results.getObject(DYN), equalTo(obj(1))); // ??
        }
    }

    @Test
    public void testSelectDynamicColComponentWhere() throws SQLException, ClassNotFoundException {
        try (Connection c = TestConnection.create(CONFIG_FILENAME);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s, %s, %s from %s where %s=%s", ROW_COMP1, COL_COMP1, COL_COMP2,
                                                                 TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName(), ROW_COMP1, 111))) {
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(2)));
            fails(() -> results.getString(ROW_COMP2));
            fails(() -> results.getLong(COL_COMP1));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(2)));
            assertThat(results.getObject(DYN), equalTo(obj(1))); // ??
            assertFalse(results.next());
        }
    }

    private void validateResult(ResultSet results, int row, int col) throws SQLException {
        assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(row)));
        assertThat(results.getString(ROW_COMP2), equalTo(rowComp2(row)));
        assertThat(results.getLong(COL_COMP1), equalTo(colComp1(col)));
        assertThat(results.getString(COL_COMP2), equalTo(colComp2(col)));
        assertThat(results.getObject(DYN), equalTo(obj(col)));
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
            t.put(tableRef, ImmutableMap.of(Cell.create(row(1), col(1)), obj(1).toByteArray(),
                                            Cell.create(row(1), col(2)), obj(2).toByteArray()));
            t.put(tableRef, ImmutableMap.of(Cell.create(row(2), col(3)), obj(3).toByteArray()));
            return null;
        });
    }

    private byte[] row(int i) {
        return EncodingUtils.add(PtBytes.toBytes((long) rowComp1(i)), PtBytes.toBytes(rowComp2(i)));
    }

    private int rowComp1(int i) {
        return i * 1000;
    }

    private String rowComp2(int i) {
        return "key" + i;
    }

    private byte[] col(int i) {
        return EncodingUtils.add(Longs.toByteArray(colComp1(i)), PtBytes.toBytes(colComp2(i)));
    }

    private long colComp1(int i) {
        return 111L * i;
    }

    private String colComp2(int i) {
        return "colComp" + i;
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

}
