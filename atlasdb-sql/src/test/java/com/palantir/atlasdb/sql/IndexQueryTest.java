package com.palantir.atlasdb.sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static com.palantir.atlasdb.sql.QueryTests.IN_MEMORY_TEST_CONFIG;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.indexing.IndexTest;
import com.palantir.atlasdb.schema.indexing.IndexTestSchema;
import com.palantir.atlasdb.schema.indexing.generated.DataTable;
import com.palantir.atlasdb.schema.indexing.generated.IndexTestTableFactory;
import com.palantir.atlasdb.sql.jdbc.AtlasJdbcDriver;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.RuntimeTransactionTask;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;

/*
schema.addTableDefinition("two_columns", new TableDefinition() {{
            rowName();
                rowComponent("id", ValueType.FIXED_LONG);
            columns();
                column("foo", "f", ValueType.FIXED_LONG);
                column("bar", "b", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("two_columns");
            rowName();
                componentFromColumn("foo", ValueType.FIXED_LONG, "foo", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);
        }});

        schema.addIndexDefinition("foo_to_id_cond", new IndexDefinition(IndexType.CELL_REFERENCING) {{
            onTable("two_columns");
            onCondition("foo", "_value > 1");
            rowName();
                componentFromColumn("foo", ValueType.FIXED_LONG, "foo", "_value");
            dynamicColumns();
                componentFromRow("id", ValueType.FIXED_LONG);

        }});
 */


public class IndexQueryTest {
    private static final TableReference DATA_TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "data");
    private static final TableReference TWO_TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "two_columns");
    private static final TableReference FOO2ID = TableReference.create(Namespace.DEFAULT_NAMESPACE, "foo_to_id_idx");     // note the postfix
    private static TransactionManager txManager;
    private static KeyValueService kvs;

    @BeforeClass
    public static void setup() throws SQLException, ClassNotFoundException {
        try (Connection ignored = QueryTests.connect(QueryTests.IN_MEMORY_TEST_CONFIG)) {
            // hack to populate AtlasJdbcDriver.getLastKnownAtlasServices()
        }
        AtlasDbServices services = AtlasJdbcDriver.getLastKnownAtlasServices();
        txManager = services.getTransactionManager();
        kvs = services.getKeyValueService();
        Schemas.truncateTablesAndIndexes(IndexTestSchema.getSchema(), kvs);
        Schemas.createTablesAndIndexes(IndexTestSchema.getSchema(), kvs);
        IndexTest.populateDataTableAndIndices(txManager);  // populates DATA_TABLE
        IndexTest.populateTwoColumnTableWithIndices(txManager);  // populates TWO_TABLE
    }

    @Test
    public void testIndex() {
        try (Connection c = QueryTests.connect(IN_MEMORY_TEST_CONFIG)) {
            Statement stmt = c.createStatement();
            ResultSet results = stmt.executeQuery(String.format("select * from %s", DATA_TABLE.getQualifiedName()));
            results.next();
            assertThat(results.getLong("id"), equalTo(1L));

            results = stmt.executeQuery(String.format("select * from %s", FOO2ID.getQualifiedName()));
            results.next();
            assertThat(results.getLong("id"), equalTo(1L));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failure running select.", e);
        }
    }

    @AfterClass
    public static void tearDown() {
        txManager.runTaskWithRetry(new RuntimeTransactionTask<Void>() {
            @Override
            public Void execute(Transaction t) {
                DataTable table = IndexTestTableFactory.of().getDataTable(t);
                table.delete(DataTable.DataRow.of(1L));
                table.delete(DataTable.DataRow.of(3L));
                return null;
            }
        });
    }
}
