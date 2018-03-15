/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;

public class SchemaMutationLockTables {
    public static final String LOCK_TABLE_PREFIX = "_locks";

    private static final Logger log = LoggerFactory.getLogger("SchemaMutationLockTables");
    private static final Predicate<String> IS_LOCK_TABLE = table -> table.startsWith(LOCK_TABLE_PREFIX);

    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;

    public SchemaMutationLockTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    public Set<TableReference> getAllLockTables() throws TException {
        Set<TableReference> lockTables = new HashSet<>();
        for (InetSocketAddress host : clientPool.getCurrentPools().keySet()) {
            lockTables.addAll(clientPool.runWithRetryOnHost(host, this::getAllLockTablesInternal));
        }
        return lockTables;
    }

    private Set<TableReference> getAllLockTablesInternal(CassandraClient client) throws TException {
        return client.describe_keyspace(config.getKeyspaceOrThrow()).getCf_defs().stream()
                .map(CfDef::getName)
                .filter(IS_LOCK_TABLE)
                .map(TableReference::createWithEmptyNamespace)
                .collect(Collectors.toSet());
    }

    public TableReference createLockTable() throws TException {
        return clientPool.runWithRetry(this::createLockTable);
    }

    private TableReference createLockTable(CassandraClient client) throws TException {
        String lockTableName = LOCK_TABLE_PREFIX + "_" + getUniqueSuffix();
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        log.info("Creating lock table {}", SafeArg.of("schemaMutationTableName", lockTable));
        createTableInternal(client, lockTable);
        return lockTable;
    }

    private String getUniqueSuffix() {
        // We replace '-' with '_' as hyphens are forbidden in Cassandra table names.
        return UUID.randomUUID().toString().replace('-', '_');
    }

    private void createTableInternal(CassandraClient client, TableReference tableRef) throws TException {
        CfDef cf = ColumnFamilyDefinitions.getStandardCfDef(
                config.getKeyspaceOrThrow(),
                CassandraKeyValueServiceImpl.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(
                config,
                client.rawClient(),
                tableRef.getQualifiedName(),
                true);
    }
}
