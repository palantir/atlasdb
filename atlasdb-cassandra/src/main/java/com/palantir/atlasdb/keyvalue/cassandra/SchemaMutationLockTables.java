package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

public class SchemaMutationLockTables {
    private final CassandraClientPool clientPool;
    private CassandraKeyValueServiceConfigManager configManager;
    private Optional<TableReference> lockTable = Optional.empty();

    public SchemaMutationLockTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfigManager configManager) {
        this.clientPool = clientPool;
        this.configManager = configManager;
    }

    private TableReference createLockTable() {
        try {
            return clientPool.run(createInternalLockTable);
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private final FunctionCheckedException<Cassandra.Client, TableReference, Exception> createInternalLockTable = client -> {
        String lockTableName = (HiddenTables.LOCK_TABLE_PREFIX + UUID.randomUUID()).replace('-','_');;
        TableReference lockTable = TableReference.createWithEmptyNamespace(lockTableName);
        createTableInternal(client, lockTable);
        return lockTable;
    };

    private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), CassandraKeyValueService.internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }

    public TableReference getOnlyTable() {
        if (!lockTable.isPresent()) {
            lockTable = Optional.of(createLockTable());
        }
        return lockTable.get();
    }
}
