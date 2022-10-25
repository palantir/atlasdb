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

import com.google.auto.service.AutoService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.dbkvs.DbAtlasDbFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetterImpl;
import com.palantir.atlasdb.keyvalue.dbkvs.cleaner.OracleNamespaceDeleter.OracleNamespaceDeleterParameters;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OracleDbTableFactory;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OraclePrefixedTableNames;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.TableValueStyleCache;
import com.palantir.atlasdb.keyvalue.dbkvs.util.DbKeyValueServiceConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.util.SqlConnectionSuppliers;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleterFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.HikariClientPoolConnectionManagers;
import com.palantir.nexus.db.pool.config.OracleConnectionConfig;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

@AutoService(NamespaceDeleterFactory.class)
public final class DbKvsNamespaceDeleterFactory implements NamespaceDeleterFactory {
    @Override
    public String getType() {
        return DbAtlasDbFactory.TYPE;
    }

    @Override
    public NamespaceDeleter createNamespaceDeleter(
            KeyValueServiceConfig config, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        DbKeyValueServiceConfig dbKeyValueServiceConfig = DbKeyValueServiceConfigs.toDbKeyValueServiceConfig(config);
        String dbType = dbKeyValueServiceConfig.ddl().type();

        if (OracleDdlConfig.TYPE.equals(dbType)) {
            return createOracleNamespaceDeleter(dbKeyValueServiceConfig, runtimeConfig);
        }
        throw new SafeIllegalArgumentException(
                "Dropping a namespace for the given DB type is not supported",
                SafeArg.of("type", dbType),
                SafeArg.of("supportedTypes", OracleDdlConfig.TYPE));
    }

    private NamespaceDeleter createOracleNamespaceDeleter(
            DbKeyValueServiceConfig config, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        OracleDdlConfig ddlConfig = (OracleDdlConfig) config.ddl();
        OracleConnectionConfig connectionConfig = (OracleConnectionConfig) config.connection();

        OracleTableNameGetter tableNameGetter = OracleTableNameGetterImpl.createDefault(ddlConfig);
        OraclePrefixedTableNames prefixedTableNames = new OraclePrefixedTableNames(tableNameGetter);
        TableValueStyleCache cache = new TableValueStyleCache();

        // DDL tables require a compaction executor service. However, namespace deletion doesn't use compaction at
        // all, so we never use the executor service.
        ExecutorService executorService = MoreExecutors.newDirectExecutorService();
        OracleDbTableFactory factory =
                new OracleDbTableFactory(ddlConfig, tableNameGetter, prefixedTableNames, cache, executorService);
        ConnectionSupplier connectionSupplier = createConnectionSupplier(config, runtimeConfig);

        OracleNamespaceDeleterParameters parameters = OracleNamespaceDeleterParameters.builder()
                .userId(connectionConfig.getDbLogin())
                .tablePrefix(ddlConfig.tablePrefix())
                .overflowTablePrefix(ddlConfig.overflowTablePrefix())
                .connectionSupplier(connectionSupplier)
                .oracleDdlTableFactory(tableReference -> factory.createDdl(tableReference, connectionSupplier))
                .tableNameGetter(tableNameGetter)
                .build();

        return new OracleNamespaceDeleter(parameters);
    }

    private ConnectionSupplier createConnectionSupplier(
            DbKeyValueServiceConfig config, Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        ConnectionManager connectionManager = HikariClientPoolConnectionManagers.create(config.connection());
        SqlConnectionSupplier sqlConnSupplier =
                SqlConnectionSuppliers.createSimpleConnectionSupplier(connectionManager, config, runtimeConfig);
        return new ConnectionSupplier(sqlConnSupplier);
    }
}
