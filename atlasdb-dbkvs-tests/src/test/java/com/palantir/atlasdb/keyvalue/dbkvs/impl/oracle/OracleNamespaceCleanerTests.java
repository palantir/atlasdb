/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.ImmutableNamespaceCleanerParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner.NamespaceCleanerParameters;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.util.file.TempFileUtils;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class OracleNamespaceCleanerTests {
    private static final String TABLE_PREFIX = "a_";
    private static final String OVERFLOW_TABLE_PREFIX = "ao_";
    private static final String ANOTHER_TABLE_PREFIX = "test";
    private static final String ANOTHER_OVERFLOW_TABLE_PREFIX = "overflow";
    private static final String TABLE_NAME_1 = "test";
    private static final String TABLE_NAME_2 = "hello";
    private static final String TABLE_NAME_3 = "world";
    private static final String TEST_USER = "testuser";
    private static final String TEST_USER_2 = "testuser2";
    private File sqliteLocation;

    @Before
    public void before() throws IOException {
        sqliteLocation = TempFileUtils.createTempFile("sqlite", "db");
        createMetadataTableIfNotExists();
    }

    @After
    public void after() {
        sqliteLocation.delete();
    }

    @Test
    public void dropAllTablesOnlyDropsTablesWithConfigPrefixesForCurrentUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();
        createDefaultTable(TABLE_NAME_1);
        createDefaultTable(TABLE_NAME_2);

        createTable(getTableName(ANOTHER_TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        createTable(getTableName(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2), TEST_USER);
        createTable(getTableName(TABLE_PREFIX, TABLE_NAME_3), TEST_USER_2);

        assertThat(countTables()).isEqualTo(7);
        namespaceCleaner.dropAllTables();
        assertThat(countTables()).isEqualTo(3);
    }

    @Test
    public void areAllTablesDroppedReturnsTrueIfNoTablesWithConfigPrefixAndUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(getTableName(ANOTHER_TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        createTable(getTableName(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2), TEST_USER);
        createTable(getTableName(TABLE_PREFIX, TABLE_NAME_3), TEST_USER_2);

        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isTrue();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTablePrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(getTableName(TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTableOverflowPrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createTable(getTableName(OVERFLOW_TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesDroppedReturnsFalseIfTablePrefixAndOverflowTablePrefixExistsForConfigUser() {
        NamespaceCleaner namespaceCleaner = createDefaultNamespaceCleaner();

        createDefaultTable(TABLE_NAME_1);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void areAllTablesDroppedClosesConnection() throws SQLException {
        List<Connection> allConnections = new ArrayList<>();
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .connectionManager(() -> {
                    Connection connection = spy(getConnectionSupplier().get());
                    allConnections.add(connection);
                    return connection;
                })
                .build());
        createDefaultTable(TABLE_NAME_1);
        createDefaultTable(TABLE_NAME_2);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        for (Connection connection : allConnections) {
            verify(connection).close();
        }
    }

    @Test
    public void dropAllTablesMakesProgressInSpiteOfFailures() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .connectionManager(getConnectionSupplier(i -> i == 3))
                .build());

        for (int i = 0; i < 10; i++) {
            createDefaultTable(TABLE_NAME_1 + i);
        }

        assertThat(countTables()).isEqualTo(20);
        assertThatThrownBy(namespaceCleaner::dropAllTables);
        assertThat(countTables()).isLessThan(20);
    }

    @Test
    public void dropAllTablesIsRetryable() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .connectionManager(getConnectionSupplier(i -> i == 3))
                .build());

        for (int i = 0; i < 10; i++) {
            createDefaultTable(TABLE_NAME_1 + i);
        }

        assertThat(countTables()).isEqualTo(20);
        assertThatThrownBy(namespaceCleaner::dropAllTables);
        assertThat(countTables()).isGreaterThan(0);

        refreshAllTableMetadata();
        namespaceCleaner.dropAllTables();
        assertThat(countTables()).isEqualTo(0);
    }

    @Test
    public void dropAllTablesDropsEverythingOwnedByUserIfPrefixEmpty() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(
                createDefaultNamespaceCleanerParameters().tablePrefix("").build());

        createDefaultTable(TABLE_NAME_1);
        createDefaultTable(TABLE_NAME_2);

        createTable(getTableName(ANOTHER_TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        createTable(getTableName(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2), TEST_USER);
        createTable(getTableName(TABLE_PREFIX, TABLE_NAME_3), TEST_USER_2);

        assertThat(countTables()).isEqualTo(7);

        namespaceCleaner.dropAllTables();

        assertThat(countTables()).isEqualTo(1);
    }

    @Test
    public void dropAllTablesDropsEverythingOwnedByUserIfOverflowPrefixEmpty() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .overflowTablePrefix("")
                .build());

        createDefaultTable(TABLE_NAME_1);
        createDefaultTable(TABLE_NAME_2);

        createTable(getTableName(ANOTHER_TABLE_PREFIX, TABLE_NAME_1), TEST_USER);
        createTable(getTableName(ANOTHER_OVERFLOW_TABLE_PREFIX, TABLE_NAME_2), TEST_USER);
        createTable(getTableName(TABLE_PREFIX, TABLE_NAME_3), TEST_USER_2);

        assertThat(countTables()).isEqualTo(7);

        namespaceCleaner.dropAllTables();

        assertThat(countTables()).isEqualTo(1);
    }

    @Test
    public void dropAllTablesClosesConnection() throws SQLException {
        List<Connection> allConnections = new ArrayList<>();
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .connectionManager(() -> {
                    Connection connection = spy(getConnectionSupplier().get());
                    allConnections.add(connection);
                    return connection;
                })
                .build());
        createDefaultTable(TABLE_NAME_1);
        createDefaultTable(TABLE_NAME_2);
        namespaceCleaner.dropAllTables();
        for (Connection connection : allConnections) {
            verify(connection).close();
        }
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThat(countTables()).isEqualTo(2);
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .tablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThat(countTables()).isEqualTo(2);
    }

    @Test
    public void areAllTablesDeletedDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.areAllTablesSuccessfullyDropped();
        assertThat(countTables()).isEqualTo(2);
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnOwner() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'; CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.dropAllTables();
        assertThat(countTables()).isEqualTo(2);
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .tablePrefix("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.dropAllTables();
        assertThat(countTables()).isEqualTo(1); // we actually do delete a table. TODO This should just list the
        // actual tables
    }

    @Test
    public void dropTablesDoesNotExecuteArbitrarySqlOnOverflowTablePrefix() {
        NamespaceCleaner namespaceCleaner = new OracleNamespaceCleaner(createDefaultNamespaceCleanerParameters()
                .userId("1'); CREATE TABLE mwahahaha (evil VARCHAR(128) PRIMARY KEY); --")
                .build());
        createDefaultTable(TABLE_NAME_1);
        assertThat(countTables()).isEqualTo(2);
        namespaceCleaner.dropAllTables();
        assertThat(countTables()).isEqualTo(2);
    }

    // Don't delete other peoples tables, including with prefix, nor other users, what happens with the empty
    // We retry correctly, that we close the connections, that we're not susceptible to injection
    private void createMetadataTableIfNotExists() {
        runWithConnection(connection -> {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS all_tables (table_name VARCHAR(128) NOT NULL, "
                    + "owner VARCHAR(32) NOT NULL, PRIMARY KEY (table_name, owner))");
            return null;
        });
    }

    private void createDefaultTable(String tableName) {
        createTable(getTableName(TABLE_PREFIX, tableName), TEST_USER);
        createTable(getTableName(OVERFLOW_TABLE_PREFIX, tableName), TEST_USER);
    }

    private void createTable(String tableName, String owner) {
        runWithConnection(connection -> {
            PreparedStatement insertAllTables = connection.prepareStatement("INSERT INTO all_tables VALUES (?, ?)");
            insertAllTables.setString(1, tableName);
            insertAllTables.setString(2, owner);
            insertAllTables.executeUpdate();

            String createTableSql = "CREATE TABLE %s (k VARCHAR(128) PRIMARY KEY, value VARCHAR(32))";
            Statement createNewTable = connection.createStatement();
            createNewTable.executeUpdate(String.format(createTableSql, tableName));
            return null;
        });
    }

    private String getTableName(String prefix, String tableName) {
        return prefix + tableName;
    }

    private int countTables() {
        return runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement("SELECT COUNT(name) as number_of_tables FROM "
                    + "sqlite_schema WHERE "
                    + "type ='table' AND name NOT LIKE 'sqlite_%' AND name != 'all_tables'");
            ResultSet resultSet = statement.executeQuery();
            return resultSet.getInt("number_of_tables");
        });
    }

    private void refreshAllTableMetadata() {
        runWithConnection(connection -> {
            PreparedStatement statement = connection.prepareStatement("DELETE FROM all_tables WHERE table_name NOT IN"
                    + " (SELECT name as table_name FROM sqlite_schema WHERE type = 'table')");
            statement.executeUpdate();
            return null;
        });
    }

    private <T> T runWithConnection(FunctionCheckedException<Connection, T, SQLException> task) {
        try (Connection connection = DriverManager.getConnection(getConnectionUrl())) {
            return task.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private NamespaceCleaner createDefaultNamespaceCleaner() {
        return new OracleNamespaceCleaner(
                createDefaultNamespaceCleanerParameters().build());
    }

    private ImmutableNamespaceCleanerParameters.Builder createDefaultNamespaceCleanerParameters() {
        return NamespaceCleanerParameters.builder()
                .tablePrefix(TABLE_PREFIX)
                .overflowTablePrefix(OVERFLOW_TABLE_PREFIX)
                .userId(TEST_USER)
                .connectionManager(getConnectionSupplier());
    }

    private Supplier<Connection> getConnectionSupplier() {
        return getConnectionSupplier(_i -> false);
    }

    private Supplier<Connection> getConnectionSupplier(Predicate<Integer> throwingPredicate) {
        AtomicInteger numberOfConnections = new AtomicInteger();
        return () -> {
            try {
                if (throwingPredicate.test(numberOfConnections.incrementAndGet())) {
                    throw new RuntimeException("induced failure");
                }
                return DriverManager.getConnection(getConnectionUrl());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private String getConnectionUrl() {
        return "jdbc:sqlite:" + sqliteLocation.getAbsolutePath();
    }
}
