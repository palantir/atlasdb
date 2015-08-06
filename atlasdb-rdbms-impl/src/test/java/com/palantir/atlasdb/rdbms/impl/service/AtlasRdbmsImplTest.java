package com.palantir.atlasdb.rdbms.impl.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnection;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnectionCallable;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsConnectionCheckedCallable;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsExecutionException;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsResultSetHandler;
import com.palantir.atlasdb.rdbms.api.service.AtlasRdbmsSchemaVersion;
import com.palantir.atlasdb.rdbms.impl.util.ResultSetHandlers;
import com.palantir.util.Nullable;

public class AtlasRdbmsImplTest {

    private AtlasRdbmsImpl db;
    private Set<String> createdSequences;
    private LinkedList<String> createdTables;

    @Before
    public void setUp() throws Exception {
        createdSequences = Sets.newHashSet();
        createdTables = Lists.newLinkedList();
        db = AtlasRdbmsTestUtils.getTestDb();
    }

    @After
    public void tearDown() throws Exception {
        for (String sequence : createdSequences) {
            dropSequence(sequence);
        }
        for (String table : createdTables) {
            dropTable(table);
        }
        db.shutdown();
    }

    private AtlasRdbmsSchemaVersion createSequence(String name, long start, long increment) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        AtlasRdbmsSchemaVersion versionAfterMigration = new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0);
        db.performSchemaMigration(current, versionAfterMigration,
                Lists.newArrayList("CREATE SEQUENCE " + name + " INCREMENT BY " + increment + " START WITH " + start));
        createdSequences.add(name);
        return versionAfterMigration;
    }

    private AtlasRdbmsSchemaVersion createTables(List<String> tablesCreated, List<String> createTablesSql) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        AtlasRdbmsSchemaVersion versionAfterMigration = new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0);
        db.performSchemaMigration(current, versionAfterMigration, createTablesSql);
        for (String tableName : tablesCreated) {
            createdTables.addFirst(tableName);
        }
        return versionAfterMigration;
    }

    private AtlasRdbmsSchemaVersion dropSequence(String name) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        AtlasRdbmsSchemaVersion versionAfterMigration = new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0);
        db.performSchemaMigration(current, versionAfterMigration, Lists.newArrayList("DROP SEQUENCE " + name));
        return versionAfterMigration;
    }

    private AtlasRdbmsSchemaVersion dropTable(String name) throws Exception {
        AtlasRdbmsSchemaVersion current = db.getDbSchemaVersion();
        AtlasRdbmsSchemaVersion versionAfterMigration = new AtlasRdbmsSchemaVersion(current.getMajorVersion() + 1, 0);
        db.performSchemaMigration(current, versionAfterMigration, Lists.newArrayList("DROP TABLE " + name));
        return versionAfterMigration;
    }

    @Test
    public void testSystemProperties() throws Exception {
        Assert.assertFalse(db.getDbSystemProperty("foo").isPresent());
        Assert.assertFalse(db.setDbSystemProperty("foo", "foo_val").isPresent());
        Nullable<String> foo = db.getDbSystemProperty("foo");
        Assert.assertTrue(foo.isPresent());
        Assert.assertEquals("foo_val", foo.get());

        Assert.assertFalse(db.getDbSystemProperty("bar").isPresent());
        Assert.assertFalse(db.setDbSystemProperty("bar", "bar_val").isPresent());
        Nullable<String> bar = db.getDbSystemProperty("bar");
        Assert.assertTrue(bar.isPresent());
        Assert.assertEquals("bar_val", bar.get());

        foo = db.setDbSystemProperty("foo", null);
        Assert.assertTrue(foo.isPresent());
        Assert.assertEquals("foo_val", foo.get());
        Assert.assertFalse(db.getDbSystemProperty("foo").isPresent());
        bar = db.getDbSystemProperty("bar");
        Assert.assertTrue(bar.isPresent());
        Assert.assertEquals("bar_val", bar.get());

    }

    @Test
    public void testCreateTables() throws Exception {
        // we don't know what the initial version might be because the db doesn't necessarily
        // get destroyed after each run
        AtlasRdbmsSchemaVersion initialVersion = db.getDbSchemaVersion();
        String createTable1 = "CREATE TABLE atlas_test_1 (string VARCHAR(512), long BIGINT, CONSTRAINT pk_test_1 PRIMARY KEY (long))";
        String createTable2 = "CREATE TABLE atlas_test_2 (blob BLOB, long BIGINT, CONSTRAINT pk_test_2 PRIMARY KEY (long), "
                + "CONSTRAINT fk_test_1_2 FOREIGN KEY (long) REFERENCES atlas_test_1 (long))";
        AtlasRdbmsSchemaVersion newVersion = createTables(Lists.newArrayList("atlas_test_1", "atlas_test_2"),
                Lists.newArrayList(createTable1, createTable2));
        Assert.assertTrue(initialVersion.compareTo(newVersion) < 0);
        Assert.assertEquals(newVersion, db.getDbSchemaVersion());

        boolean rowsExist = db.runWithDbConnectionInTransaction(new AtlasRdbmsConnectionCallable<Boolean>() {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT * FROM atlas_test_1", ResultSetHandlers.ROWS_EXIST);
            }
        });
        // Query should run successfully and there should be no rows
        Assert.assertFalse(rowsExist);

        rowsExist = db.runWithDbConnectionInTransaction(new AtlasRdbmsConnectionCallable<Boolean>() {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT * FROM atlas_test_2", ResultSetHandlers.ROWS_EXIST);
            }
        });
        // Query should run successfully and there should be no rows
        Assert.assertFalse(rowsExist);
    }

    @Test
    public void testSimpleQueries() throws Exception {
        String createTable1 = "CREATE TABLE atlas_test_1 (string VARCHAR(512), long BIGINT, CONSTRAINT pk_test_1 PRIMARY KEY (long))";
        String createTable2 = "CREATE TABLE atlas_test_2 (blob BLOB, long BIGINT, CONSTRAINT pk_test_2 PRIMARY KEY (long), "
                + "CONSTRAINT fk_test_1_2 FOREIGN KEY (long) REFERENCES atlas_test_1 (long))";
        createTables(Lists.newArrayList("atlas_test_1", "atlas_test_2"), Lists.newArrayList(createTable1, createTable2));

        final AtlasRdbmsResultSetHandler<String> rsh = new AtlasRdbmsResultSetHandler<String>() {
            @Override
            public String handle(ResultSet rs) throws SQLException {
                if (!rs.next()) {
                    return null;
                }
                return rs.getString("string");
            }
        };

        String value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                c.update("INSERT INTO atlas_test_1 (string, long) VALUES (?, ?)", "foo", 1L);
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals("foo", value);

        value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals("foo", value);

        value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                c.update("UPDATE atlas_test_1 SET string = ? WHERE long = ?", "bar", 1L);
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals("bar", value);

        value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals("bar", value);


        value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                c.update("DELETE FROM atlas_test_1 WHERE long = ?", 1L);
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals(null, value);

        value = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<String>() {
            @Override
            public String call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
            }
        });
        Assert.assertEquals(null, value);


    }

    @Test
    public void testBatchQueries() throws Exception {
        String createTable1 = "CREATE TABLE atlas_test_1 (string VARCHAR(512), long BIGINT, CONSTRAINT pk_test_1 PRIMARY KEY (long))";
        String createTable2 = "CREATE TABLE atlas_test_2 (blob BLOB, long BIGINT, CONSTRAINT pk_test_2 PRIMARY KEY (long), "
                + "CONSTRAINT fk_test_1_2 FOREIGN KEY (long) REFERENCES atlas_test_1 (long))";
        createTables(Lists.newArrayList("atlas_test_1", "atlas_test_2"), Lists.newArrayList(createTable1, createTable2));

        final Map<Long, String> toInsert = Maps.newHashMap();
        for (long i = 0; i < 10; i++) {
            toInsert.put(i, "foo" + i);
        }

        final AtlasRdbmsResultSetHandler<Map<Long, String>> rsh = new AtlasRdbmsResultSetHandler<Map<Long, String>>() {
            @Override
            public Map<Long, String> handle(ResultSet rs) throws SQLException {
                Map<Long, String> values = Maps.newHashMap();
                while (rs.next()) {
                    values.put(rs.getLong("long"), rs.getString("string"));
                }
                return values;
            }
        };

        Map<Long, String> batchValues = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<Map<Long, String>>() {
            @Override
            public Map<Long, String> call(AtlasRdbmsConnection c) throws Exception {
                Object[][] values = new Object[10][2];
                int i = 0;
                for (Entry<Long, String> entry : toInsert.entrySet()) {
                    values[i][0] = entry.getValue();
                    values[i][1] = entry.getKey();
                    i++;
                }
                int[] affectedRows = c.batch("INSERT INTO atlas_test_1 (string, long) VALUES (?, ?)", values);
                for (int rows : affectedRows) {
                    if (rows != 1) {
                        throw new SQLException("Failed to insert a row");
                    }
                }
                return c.query("SELECT * FROM atlas_test_1", rsh);
            }
        });
        Assert.assertEquals(toInsert, batchValues);

        batchValues = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<Map<Long, String>>() {
            @Override
            public Map<Long, String> call(AtlasRdbmsConnection c) throws Exception {
                return c.query("SELECT * FROM atlas_test_1", rsh);
            }
        });
        Assert.assertEquals(toInsert, batchValues);
    }

    @Test
    public void testInvalidQueryInTransaction() throws Exception {
        String createTable1 = "CREATE TABLE atlas_test_1 (string VARCHAR(512), long BIGINT, CONSTRAINT pk_test_1 PRIMARY KEY (long))";
        String createTable2 = "CREATE TABLE atlas_test_2 (blob BLOB, long BIGINT, CONSTRAINT pk_test_2 PRIMARY KEY (long), "
                + "CONSTRAINT fk_test_1_2 FOREIGN KEY (long) REFERENCES atlas_test_1 (long))";
        createTables(Lists.newArrayList("atlas_test_1", "atlas_test_2"), Lists.newArrayList(createTable1, createTable2));

        final AtlasRdbmsResultSetHandler<String> rsh = new AtlasRdbmsResultSetHandler<String>() {
            @Override
            public String handle(ResultSet rs) throws SQLException {
                if (!rs.next()) {
                    return null;
                }
                return rs.getString("string");
            }
        };

        try {
            db.runWithDbConnectionInTransaction(new AtlasRdbmsConnectionCallable<Void>() {
                @Override
                public Void call(AtlasRdbmsConnection c) throws Exception {
                    try {
                        c.update("INSERT INTO atlas_test_2 (blob, long) VALUES (?, ?)", new byte[] {1, 2, 3}, 1L);
                        throw new IllegalStateException("Can't insert due to foreign key constraint");
                    } catch (SQLException e) {
                        if (!c.getError().isPresent()) {
                            throw new IllegalStateException("Failed to capture DB error");
                        }
                        if (!e.equals(c.getError().get())) {
                            throw new IllegalStateException("Captured wrong DB error");
                        }
                        String value = c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
                        if (value != null) {
                            throw new IllegalStateException("Actually should have been able to insert value");
                        }
                        throw e;
                    }
                }
            });
            Assert.fail("Can't insert due to foreign key constraint");
        } catch (AtlasRdbmsExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof SQLException)) {
                throw new AssertionError(cause.getMessage());
            }
        }
    }

    @Test
    public void testInvalidQueryNotInTransaction() throws Exception {
        String createTable1 = "CREATE TABLE atlas_test_1 (string VARCHAR(512), long BIGINT, CONSTRAINT pk_test_1 PRIMARY KEY (long))";
        String createTable2 = "CREATE TABLE atlas_test_2 (blob BLOB, long BIGINT, CONSTRAINT pk_test_2 PRIMARY KEY (long), "
                + "CONSTRAINT fk_test_1_2 FOREIGN KEY (long) REFERENCES atlas_test_1 (long))";
        createTables(Lists.newArrayList("atlas_test_1", "atlas_test_2"), Lists.newArrayList(createTable1, createTable2));

        final AtlasRdbmsResultSetHandler<String> rsh = new AtlasRdbmsResultSetHandler<String>() {
            @Override
            public String handle(ResultSet rs) throws SQLException {
                if (!rs.next()) {
                    return null;
                }
                return rs.getString("string");
            }
        };

        try {
            db.runWithDbConnection(new AtlasRdbmsConnectionCallable<Void>() {
                @Override
                public Void call(AtlasRdbmsConnection c) throws Exception {
                    try {
                        c.update("INSERT INTO atlas_test_2 (blob, long) VALUES (?, ?)", new byte[] {1, 2, 3}, 1L);
                        throw new IllegalStateException("Can't insert due to foreign key constraint");
                    } catch (SQLException e) {
                        if (!c.getError().isPresent()) {
                            throw new IllegalStateException("Failed to capture DB error");
                        }
                        if (!e.equals(c.getError().get())) {
                            throw new IllegalStateException("Captured wrong DB error");
                        }
                        String value = c.query("SELECT string FROM atlas_test_1 WHERE long = ?", rsh, 1L);
                        if (value != null) {
                            throw new IllegalStateException("Actually should have been able to insert value");
                        }
                        throw e;
                    }
                }
            });
            Assert.fail("Can't insert due to foreign key constraint");
        } catch (AtlasRdbmsExecutionException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof SQLException)) {
                throw new AssertionError(cause.getMessage());
            }
        }
    }

    @Test
    public void testAutoCommit() throws Exception {
        boolean isAutoCommit = db.runWithDbConnection(new AtlasRdbmsConnectionCallable<Boolean>() {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.isAutoCommit();
            }
        });
        Assert.assertTrue(isAutoCommit);
        isAutoCommit = db.runWithDbConnection(new AtlasRdbmsConnectionCheckedCallable<Boolean, RuntimeException>(RuntimeException.class) {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.isAutoCommit();
            }
        });
        Assert.assertTrue(isAutoCommit);

        isAutoCommit = db.runWithDbConnectionInTransaction(new AtlasRdbmsConnectionCallable<Boolean>() {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.isAutoCommit();
            }
        });
        Assert.assertFalse(isAutoCommit);
        isAutoCommit = db.runWithDbConnectionInTransaction(new AtlasRdbmsConnectionCheckedCallable<Boolean, RuntimeException>(RuntimeException.class) {
            @Override
            public Boolean call(AtlasRdbmsConnection c) throws Exception {
                return c.isAutoCommit();
            }
        });
        Assert.assertFalse(isAutoCommit);
    }
}
