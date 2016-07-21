package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class UniqueSchemaMutationLockTable {
    private final SchemaMutationLockTables schemaMutationLockTables;

    public static UniqueSchemaMutationLockTable create(SchemaMutationLockTables schemaMutationLockTables) {
        UniqueSchemaMutationLockTable uniqueSchemaMutationLockTable = new UniqueSchemaMutationLockTable(schemaMutationLockTables);
        uniqueSchemaMutationLockTable.getOnlyTable();
        return uniqueSchemaMutationLockTable;
    }

    protected UniqueSchemaMutationLockTable(SchemaMutationLockTables schemaMutationLockTables) {
        this.schemaMutationLockTables = schemaMutationLockTables;
    }

    public TableReference getOnlyTable() {
        try {
            return ensureLockTableExists();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private final TableReference ensureLockTableExists() throws TException {
            Set<TableReference> tables = schemaMutationLockTables.getAllLockTables();

            if (tables.isEmpty()) {
                TableReference lockTable =  schemaMutationLockTables.createLockTable(UUID.randomUUID());
                Set<TableReference> lockTables = schemaMutationLockTables.getAllLockTables();
                if (schemaMutationLockTables.getAllLockTables().size() > 1) {
                    throw new IllegalStateException(
                            "Multiple schema mutation lock tables have been created.\n" +
                                    "This happens when multiple nodes have themselves as lockLeader in the configuration.\n" +
                                    "Please ensure the lockLeader is the same for each node, stop all Atlas clients using " +
                                    "this keyspace, restart your cassandra cluster and delete all created schema mutation lock tables.\n" +
                                    "The tables that clashed were: " + lockTables);
                }
                return lockTable;
            }

            return Iterables.getOnlyElement(tables);
    }
}
