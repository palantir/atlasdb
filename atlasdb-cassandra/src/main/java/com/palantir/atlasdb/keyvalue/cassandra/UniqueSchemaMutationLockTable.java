package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class UniqueSchemaMutationLockTable {
    private static final Predicate<String> IS_LOCK_TABLE = table -> table.startsWith(HiddenTables.LOCK_TABLE_PREFIX);
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;
    private final SchemaMutationLockTables schemaMutationLockTables;


    public UniqueSchemaMutationLockTable(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
        schemaMutationLockTables = new SchemaMutationLockTables(this.clientPool, this.config);
    }

    public TableReference getOnlyTable() {
        return getOrCreateLockTable();
    }

    private TableReference getOrCreateLockTable() {
        try {
            return ensureLockTableExists();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private final TableReference ensureLockTableExists() throws TException {
            Set<TableReference> tables = schemaMutationLockTables.getAllLockTables();

            if (tables.isEmpty()) {
                return schemaMutationLockTables.createLockTable(UUID.randomUUID());
            }

            return Iterables.getOnlyElement(tables);
    }
}
