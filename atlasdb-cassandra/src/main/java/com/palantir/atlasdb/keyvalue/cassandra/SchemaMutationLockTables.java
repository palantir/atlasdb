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

    public final TableReference createLockTable(UUID uuid) throws TException {
        return clientPool.run(client -> createInternalLockTable(client, uuid));
    }

    private final TableReference createInternalLockTable(Cassandra.Client client, UUID uuid) throws TException {
        String lockTableName = HiddenTables.LOCK_TABLE_PREFIX + uuid.toString().replace('-','_');
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        createTableInternal(client, lockTable);
        return lockTable;
    };

    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), CassandraKeyValueService.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }
}
