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

import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceCleaner.NamespaceCleanerParameters;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.HikariClientPoolConnectionManagers;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class OracleNamespaceCleanerIntegrationTest {
    private static final SafeLogger log = SafeLoggerFactory.get(OracleNamespaceCleanerIntegrationTest.class);

    private static final String LIST_ALL_TABLES =
            "SELECT COUNT(table_name) AS total FROM all_tables WHERE owner = ? AND table_name LIKE ?";
    private static final String TABLE_NAME_ONE = "tablenameone";
    private static final String TABLE_NAME_TWO = "tablenametwo";

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsOracleTestSuite::createKvs);

    private KeyValueService keyValueService;
    private DbKeyValueServiceConfig dbKeyValueServiceConfig;
    private OracleDdlConfig oracleDdlConfig;
    private NamespaceCleaner namespaceCleaner;
    private ConnectionManager connectionManager;

    @Before
    public void before() {
        keyValueService = TRM.getDefaultKvs();
        dbKeyValueServiceConfig = DbKvsOracleTestSuite.getKvsConfig();
        oracleDdlConfig = (OracleDdlConfig) dbKeyValueServiceConfig.ddl();
        connectionManager =
                HikariClientPoolConnectionManagers.createShared(dbKeyValueServiceConfig.connection(), 1, 60);
        namespaceCleaner = new OracleNamespaceCleaner(NamespaceCleanerParameters.builder()
                .tablePrefix(oracleDdlConfig.tablePrefix())
                .overflowTablePrefix(oracleDdlConfig.overflowTablePrefix())
                .connectionManager(connectionManager::getConnectionUnchecked)
                .userId(dbKeyValueServiceConfig.connection().getDbLogin())
                .build());
    }

    @After
    public void after() {
        connectionManager.closeUnchecked();
    }

    @Test
    public void dropNamespaceDropsAllTablesAndOverflowTablesWithConfigPrefix() {
        int kvsTables = getNumberOfTables();
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertThat(getNumberOfTables() - kvsTables).isEqualTo(4);

        namespaceCleaner.dropAllTables();

        assertThat(getNumberOfTables()).isEqualTo(0);
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsFalseIfTablesRemain() {
        createTableAndOverflowTable(TABLE_NAME_ONE);
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isFalse();
    }

    @Test
    public void hasNamespaceSuccessfullyDroppedReturnsTrueIfNoTablesRemain() {
        int kvsTables = getNumberOfTables();
        createTableAndOverflowTable(TABLE_NAME_ONE);
        createTableAndOverflowTable(TABLE_NAME_TWO);
        assertThat(getNumberOfTables() - kvsTables).isEqualTo(4);

        namespaceCleaner.dropAllTables();
        assertThat(namespaceCleaner.areAllTablesSuccessfullyDropped()).isTrue();
    }

    private void createTableAndOverflowTable(String tableName) {
        keyValueService.createTable(
                getTableReference(tableName),
                TableMetadata.builder()
                        .nameLogSafety(LogSafety.SAFE)
                        .columns(new ColumnMetadataDescription())
                        .build()
                        .persistToBytes());
    }

    private TableReference getTableReference(String tableName) {
        return dbKeyValueServiceConfig
                .namespace()
                .map(namespace -> TableReference.create(Namespace.create(namespace), tableName))
                .orElseGet(() -> TableReference.createWithEmptyNamespace(tableName));
    }

    private int getNumberOfTables() {
        return getNumberOfTables(oracleDdlConfig.tablePrefix())
                + getNumberOfTables(oracleDdlConfig.overflowTablePrefix());
    }

    private int getNumberOfTables(String prefix) {
        try (Connection connection = connectionManager.getConnection()) {
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM all_tables");
            // statement.setString(1, dbKeyValueServiceConfig.connection().getDbLogin());
            // statement.setString(2, prefix + "%");
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            for (int row = 0; resultSet.next(); row++) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = resultSet.getString(i);
                    log.info(
                            "Column",
                            SafeArg.of("rowNumber", row),
                            SafeArg.of("columnValue", columnValue),
                            SafeArg.of("columnName", rsmd.getColumnName(i)));
                }
            }
            return 1;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
