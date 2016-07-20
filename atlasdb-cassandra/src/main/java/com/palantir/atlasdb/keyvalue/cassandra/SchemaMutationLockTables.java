package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

public class SchemaMutationLockTables {
    private static final Predicate<String> IS_LOCK_TABLE = table -> table.startsWith(HiddenTables.LOCK_TABLE_PREFIX);
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;


    public SchemaMutationLockTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    private final FunctionCheckedException<Cassandra.Client, TableReference, Exception> ensureLockTableExists() {
        return client -> {
            Set<TableReference> tables = getAllLockTables(client);

            if (tables.isEmpty()) {
                return createInternalLockTable(client);
            }

            return Iterables.getOnlyElement(tables);
        };
    }

    private Set<TableReference> getAllLockTables(Cassandra.Client client) throws TException {
        return client.describe_keyspace(config.keyspace()).getCf_defs().stream()
                .map(CfDef::getName)
                .filter(IS_LOCK_TABLE)
                .map(TableReference::createUnsafe)
                .collect(Collectors.toSet());
    }

    private TableReference getOrCreateLockTable() {
        try {
            return clientPool.run(ensureLockTableExists());
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private final TableReference createInternalLockTable(Cassandra.Client client) throws TException {
        String lockTableName = (HiddenTables.LOCK_TABLE_PREFIX + UUID.randomUUID()).replace('-','_');;
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        createTableInternal(client, lockTable);
        return lockTable;
    };

    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), CassandraKeyValueService.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }

    public TableReference getOnlyTable() {
            return getOrCreateLockTable();
    }
}
