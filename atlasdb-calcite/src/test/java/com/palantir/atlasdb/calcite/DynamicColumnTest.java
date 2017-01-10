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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

import static com.palantir.atlasdb.calcite.QueryTests.assertFails;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.calcite.proto.TestObject;
import com.palantir.atlasdb.calcite.proto.TestProtobufs;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class DynamicColumnTest {
    private static final String ROW_COMP1 = "rowComp1";  //long
    private static final String ROW_COMP2 = "rowComp2";  //string
    private static final String COL_COMP1 = "colComp1";  //long
    private static final String COL_COMP2 = "colComp2";  //string
    private static final String DYN_VALUE_NAME = "val";
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "dyntable");

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        cleanup();
        AtlasDbServices services = AtlasJdbcTestSuite.getAtlasDbServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();
        fillDynTable(txm, kvs);
    }

    @AfterClass
    public static void cleanup() {
        AtlasJdbcTestSuite
                .getAtlasDbServices()
                .getKeyValueService()
                .dropTable(TABLE);
    }

    private static void fillDynTable(TransactionManager txm, KeyValueService kvs) {
        TableDefinition tableDef = new TableDefinition() {{
            rowName();
                rowComponent(ROW_COMP1,    ValueType.FIXED_LONG);
                rowComponent(ROW_COMP2,    ValueType.STRING);
            dynamicColumns();
                columnComponent(COL_COMP1, ValueType.FIXED_LONG);
                columnComponent(COL_COMP2, ValueType.STRING);
            value(TestProtobufs.TestObject.class);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};

        final TableMetadata tableMetadata = tableDef.toTableMetadata();
        kvs.createTable(TABLE, tableMetadata.persistToBytes());
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(TABLE, ImmutableMap.of(
                    Cell.create(row(1), col(1)), TestObject.seededBy(1).toByteArray(),
                    Cell.create(row(1), col(2)), TestObject.seededBy(2).toByteArray()));
            t.put(TABLE, ImmutableMap.of(Cell.create(row(2), col(3)), TestObject.seededBy(3).toByteArray()));
            return null;
        });
    }

    @Test
    public void testSelectDynamicAll() throws SQLException, ClassNotFoundException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
             ResultSet results = stmt.executeQuery(
                     String.format("select * from \"%s\"", TABLE.getQualifiedName()));
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
    public void testSelectDynamicRowComponent()
            throws SQLException, ClassNotFoundException, InvalidProtocolBufferException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
             ResultSet results = stmt.executeQuery(
                     String.format("select %s, %s from \"%s\"", ROW_COMP2, DYN_VALUE_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getLong(ROW_COMP1));
            assertThat(results.getString(ROW_COMP2), equalTo(rowComp2(1)));
            assertFails(() -> results.getLong(COL_COMP1));
            assertFails(() -> results.getString(COL_COMP2));
            assertThat(TestObject.from(results.getObject(DYN_VALUE_NAME)), equalTo(TestObject.seededBy(1)));
        }
    }

    @Test
    public void testSelectDynamicColComponent() throws SQLException, ClassNotFoundException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
             ResultSet results = stmt.executeQuery(
                     String.format("select %s, %s from \"%s\"", COL_COMP1, DYN_VALUE_NAME, TABLE.getQualifiedName()));
            results.next();
            assertFails(() -> results.getLong(ROW_COMP1));
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getLong(COL_COMP1), equalTo(colComp1(1)));
            assertFails(() -> results.getString(COL_COMP2));
            assertThat(TestObject.from(results.getObject(DYN_VALUE_NAME)), equalTo(TestObject.seededBy(1)));
        }
    }

    @Test
    public void testSelectDynamicColComponentWhere() throws SQLException, ClassNotFoundException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s, %s, %s from \"%s\" where %s = %s",
                            ROW_COMP1, COL_COMP1, COL_COMP2, TABLE.getQualifiedName(), ROW_COMP1, rowComp1(1)));
            results.next();
            assertThat(results.getLong(ROW_COMP1), equalTo(rowComp1(1)));
            assertFails(() -> results.getString(ROW_COMP2));
            assertThat(results.getLong(COL_COMP1), equalTo(colComp1(1)));
            assertThat(results.getString(COL_COMP2), equalTo(colComp2(1)));
            assertFails(() -> results.getBytes(DYN_VALUE_NAME));
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
        assertThat(TestObject.from(results.getObject(DYN_VALUE_NAME)), equalTo(TestObject.seededBy(col)));
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
}
