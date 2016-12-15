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
import static org.hamcrest.Matchers.is;

import static com.palantir.atlasdb.calcite.QueryTests.assertFails;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.calcite.proto.TestObject;
import com.palantir.atlasdb.calcite.proto.TestProtobufs;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;

public class ProtobufTest {
    private static final String ROW = "row";
    private static final String COL1 = "col1";
    private static final String COL2 = "col2";
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "prototable");

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        cleanup();
        AtlasDbServices services = AtlasJdbcTestSuite.getAtlasDbServices();
        TransactionManager txm = services.getTransactionManager();
        KeyValueService kvs = services.getKeyValueService();

        TableDefinition tableDef = new TableDefinition() {{
            rowName();
                rowComponent(ROW,    ValueType.FIXED_LONG);
            columns();
                column(COL1, COL1, TestProtobufs.TestObject.class);
                column(COL2, COL2, TestProtobufs.TestObject.class);
            conflictHandler(ConflictHandler.IGNORE_ALL);
            sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING);
        }};

        final TableMetadata tableMetadata = tableDef.toTableMetadata();
        kvs.createTable(TABLE, tableMetadata.persistToBytes());
        txm.runTaskThrowOnConflict((TransactionTask<Void, RuntimeException>) t -> {
            t.put(TABLE, ImmutableMap.of(
                    Cell.create(row(1), COL1.getBytes()), TestObject.seededBy(1).toByteArray(),
                    Cell.create(row(1), COL2.getBytes()), TestObject.seededBy(2).toByteArray(),
                    Cell.create(row(2), COL1.getBytes()), TestObject.seededBy(3).toByteArray()));
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

    @Test
    public void canSelectProtobufs() throws SQLException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select * from \"%s\"", TABLE.getQualifiedName()));
            results.next();
            assertThat(TestObject.from(results.getObject(COL1)), is(TestObject.seededBy(1)));
            assertThat(TestObject.from(results.getObject(COL2)), is(TestObject.seededBy(2)));
            results.next();
            assertThat(TestObject.from(results.getObject(COL1)), is(TestObject.seededBy(3)));
            assertThat(results.next(), equalTo(false));
        }
    }

    @Test
    public void canSelectProtobufFields() throws SQLException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s['id'] as id, %s['valid'] as valid from \"%s\"", COL1, COL2, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getObject("id"), is(TestObject.seededBy(1).getId()));
            assertThat(results.getObject("valid"), is(TestObject.seededBy(2).getValid()));
            assertFails(() -> results.getLong("id"));
            assertFails(() -> results.getBoolean("valid"));
            results.next();
            assertThat(results.getObject("id"), is(TestObject.seededBy(3).getId()));
            assertFails(() -> results.getLong("id"));
            assertThat(results.next(), equalTo(false));
        }
    }

    @Test
    public void canCastProtobufFields() throws SQLException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery(
                    String.format("select CAST(%s['id'] as BIGINT) as id, "
                                    + "CAST(%s['valid'] as BOOLEAN) as valid from \"%s\"",
                            COL1, COL2, TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getObject("id"), is(TestObject.seededBy(1).getId()));
            assertThat(results.getObject("valid"), is(TestObject.seededBy(2).getValid()));
            assertThat(results.getLong("id"), is(TestObject.seededBy(1).getId()));
            assertThat(results.getBoolean("valid"), is(TestObject.seededBy(2).getValid()));
            results.next();
            assertThat(results.getObject("id"), is(TestObject.seededBy(3).getId()));
            assertThat(results.getLong("id"), is(TestObject.seededBy(3).getId()));
            assertThat(results.next(), equalTo(false));
        }
    }

    @Test
    public void canUseProtobufFieldsInWhereClauses() throws SQLException {
        try (Connection conn = AtlasJdbcTestSuite.connect()) {
            Statement stmt = conn.createStatement();
            long objId1 = TestObject.seededBy(1).getId();
            ResultSet results = stmt.executeQuery(
                    String.format("select %s['id'] as id, %s['valid'] as valid from \"%s\" "
                                    + "WHERE CAST(%s['id'] as BIGINT) = %s",
                            COL1, COL2, TABLE.getQualifiedName(), COL1, objId1));
            results.next();
            assertThat(results.getObject("id"), is(TestObject.seededBy(1).getId()));
            assertThat(results.getObject("valid"), is(TestObject.seededBy(2).getValid()));
            assertThat(results.next(), equalTo(false));
        }
    }

    private static byte[] row(int i) {
        return ValueType.FIXED_LONG.convertFromJava(i * 1000L);
    }
}
