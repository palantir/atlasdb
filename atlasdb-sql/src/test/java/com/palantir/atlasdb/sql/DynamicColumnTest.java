package com.palantir.atlasdb.sql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

import static com.palantir.atlasdb.sql.QueryTests.assertFails;

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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TestPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.TestSchema;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class DynamicColumnTest {
    private static final String ROW_COMP1 = "rowComp1";  //long
    private static final String ROW_COMP2 = "rowComp2";  //string
    private static final String COL_COMP1 = "colComp1";  //long
    private static final String COL_COMP2 = "colComp2";  //string
    public static final String DYN_VALUE_NAME = "value";

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        try (Connection ignored = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG)) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();
        fillDynTable(txm, kvs);
    }

    @Test
    public void testSelectDynamicAll() throws SQLException, ClassNotFoundException {
        try (Connection c = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG);
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
        try (Connection c = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s,%s from %s", ROW_COMP2, DYN_VALUE_NAME,
                     TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()))) {
            results.next();
            assertFails(() -> results.getLong(ROW_COMP1));
            assertThat(results.getString(ROW_COMP2), equalTo(rowComp2(1)));
            assertFails(() -> results.getLong(COL_COMP1));
            assertFails(() -> results.getString(COL_COMP2));
            assertThat(results.getObject(DYN_VALUE_NAME), equalTo(obj(1)));
        }
    }

    @Test
    public void testSelectDynamicColComponent() throws SQLException, ClassNotFoundException {
        try (Connection c = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s,%s from %s", COL_COMP2, DYN_VALUE_NAME,
                     TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName()))) {
            results.next();
            assertFails(() -> results.getLong(ROW_COMP1));
            assertFails(() -> results.getString(ROW_COMP2));
            assertFails(() -> results.getLong(COL_COMP1));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(1)));
            assertThat(results.getObject(DYN_VALUE_NAME), equalTo(obj(1)));
        }
    }

    @Test
    public void testSelectDynamicColComponentWhere() throws SQLException, ClassNotFoundException {
        try (Connection c = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG);
             Statement stmt = c.createStatement();
             ResultSet results = stmt.executeQuery(String.format("select %s, %s, %s from %s where %s = %s", ROW_COMP1, COL_COMP1, COL_COMP2,
                     TestSchema.DYNAMIC_COLUMN_TABLE.getQualifiedName(), ROW_COMP1, rowComp1(1)))) {
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(1)));
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getLong(COL_COMP1), equalTo(colComp1(1)));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(1)));
            assertFails(() -> results.getObject(DYN_VALUE_NAME));
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(1)));
            assertThat(results.getLong(COL_COMP1), equalTo(colComp1(2)));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(2)));
            assertFalse(results.next());
        }
    }

    private void validateResult(ResultSet results, int row, int col) throws SQLException {
        assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(row)));
        assertThat(results.getString(ROW_COMP2), equalTo(rowComp2(row)));
        assertThat(results.getLong(COL_COMP1), equalTo(colComp1(col)));
        assertThat(results.getString(COL_COMP2), equalTo(colComp2(col)));
        assertThat(results.getObject(DYN_VALUE_NAME), equalTo(obj(col)));
    }

    /** Dummy table with dynamic columns
     */
    private static void fillDynTable(TransactionManager txm, KeyValueService kvs) {
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

    private static byte[] row(int i) {
        return EncodingUtils.add(ValueType.FIXED_LONG.convertFromJava(rowComp1(i)), PtBytes.toBytes(rowComp2(i)));
    }

    private static long rowComp1(int i) {
        return i * 1000;
    }

    private static String rowComp2(int i) {
        return "key" + i;
    }

    private static byte[] col(int i) {
        return EncodingUtils.add(ValueType.FIXED_LONG.convertFromJava(colComp1(i)), PtBytes.toBytes(colComp2(i)));
    }

    private static long colComp1(int i) {
        return 111L * i;
    }

    private static String colComp2(int i) {
        return "colComp" + i;
    }

    private static TestPersistence.TestObject obj(int i) {
        return TestPersistence.TestObject.newBuilder()
                .setId(10L + i)
                .setDeleted(0L)
                .setDataEventId(100L * i)
                .setType(543L)
                .setIsGroup(false)
                .build();
    }

}
