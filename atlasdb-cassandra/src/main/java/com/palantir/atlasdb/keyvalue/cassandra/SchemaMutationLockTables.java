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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SchemaMutationLockTables {
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;
    private static final Predicate<String> IS_LOCK_TABLE = table -> table.startsWith(HiddenTables.LOCK_TABLE_PREFIX);

    public SchemaMutationLockTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    public Set<TableReference> getAllLockTables() throws TException {
        return clientPool.run(this::getAllLockTablesInternal);
    }

    private Set<TableReference> getAllLockTablesInternal(Cassandra.Client client) throws TException {
        return client.describe_keyspace(config.keyspace()).getCf_defs().stream()
                .map(CfDef::getName)
                .filter(IS_LOCK_TABLE)
                .map(TableReference::createUnsafe)
                .collect(Collectors.toSet());
    }

    public TableReference createLockTable(UUID uuid) throws TException {
        return clientPool.run(client -> createInternalLockTable(client, uuid));
    }

    private final TableReference createInternalLockTable(Cassandra.Client client, UUID uuid) throws TException {
        String lockTableName = HiddenTables.LOCK_TABLE_PREFIX + "_" + uuid.toString().replace('-','_');
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        createTableInternal(client, lockTable);
        return lockTable;
    }

    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), CassandraKeyValueService.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }
}
