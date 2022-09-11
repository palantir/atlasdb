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

package com.palantir.atlasdb.keyvalue.dbkvs.cleaner;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.common.base.FunctionCheckedException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.function.Supplier;
import org.immutables.value.Value;

public class OracleNamespaceCleaner implements NamespaceCleaner {
    private static final String LIST_ALL_TABLES =
            "SELECT table_name FROM all_tables WHERE owner = ? AND (table_name LIKE ? OR table_name LIKE ?)";

    private final Supplier<Connection> connectionManager;
    private final String tablePrefix;
    private final String overflowTablePrefix;
    private final String userId;

    public OracleNamespaceCleaner(NamespaceCleanerParameters parameters) {
        this.tablePrefix = parameters.tablePrefix();
        this.overflowTablePrefix = parameters.overflowTablePrefix();
        this.userId = parameters.userId();
        this.connectionManager = parameters.connectionManager();
    }

    @Override
    public void dropAllTables() {
        Set<String> tableNamesToDrop = getAllTableNamesToDrop();
        dropAllTablesFromList(tableNamesToDrop);
    }

    private Set<String> getAllTableNamesToDrop() {
        return runWithConnection(connection -> {
            ResultSet resultSet = getAllTables(connection);
            ImmutableSet.Builder<String> tablesNames = ImmutableSet.builder();
            while (resultSet.next()) {
                tablesNames.add(resultSet.getString("table_name"));
            }
            return tablesNames.build();
        });
    }

    @Override
    public boolean areAllTablesSuccessfullyDropped() {
        return runWithConnection(connection -> !getAllTables(connection).next());
    }

    private ResultSet getAllTables(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(LIST_ALL_TABLES);
        statement.setString(1, userId);
        statement.setString(2, withWildcardSuffix(tablePrefix));
        statement.setString(3, withWildcardSuffix(overflowTablePrefix));
        return statement.executeQuery();
    }

    private void dropAllTablesFromList(Set<String> tableNames) {
        for (String tableName : tableNames) {
            runWithConnection(connection -> {
                Statement statement = connection.createStatement();
                statement.executeUpdate(String.format("DROP TABLE %s", tableName)); // No PURGE NOR CASCADE
                // CONSTRAINTS. INLINED becuase of errorprone
                return null;
            });
            // There is no IF EXISTS. DDL commands perform an implicit commit. If we fail, we should just retry by
            // dropping the namespace again!
        }
    }

    private <T> T runWithConnection(FunctionCheckedException<Connection, T, SQLException> task) {
        try (Connection connection = connectionManager.get()) {
            return task.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String withWildcardSuffix(String tableName) {
        return tableName + "%";
    }

    @Override
    public void close() throws IOException {
        // try {
        //     connectionManager.close();
        // } catch (SQLException e) {
        //     throw new IOException(e);
        // }
    }

    @Value.Immutable
    public interface NamespaceCleanerParameters {
        String tablePrefix();

        String overflowTablePrefix();

        String userId();

        Supplier<Connection> connectionManager();

        static ImmutableNamespaceCleanerParameters.Builder builder() {
            return ImmutableNamespaceCleanerParameters.builder();
        }
    }
}
