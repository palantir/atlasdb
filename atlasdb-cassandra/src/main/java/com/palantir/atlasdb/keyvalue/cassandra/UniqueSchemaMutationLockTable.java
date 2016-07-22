package com.palantir.atlasdb.keyvalue.cassandra;

import static com.google.common.base.Preconditions.*;

import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class UniqueSchemaMutationLockTable {
    private final SchemaMutationLockTables schemaMutationLockTables;
    private final LockLeader lockLeader;

    public UniqueSchemaMutationLockTable(SchemaMutationLockTables schemaMutationLockTables, LockLeader lockLeader) {
        this.schemaMutationLockTables = schemaMutationLockTables;
        this.lockLeader = checkNotNull(lockLeader, "We must know if we are the lock leader");
    }

    public TableReference getOnlyTable() {
        try {
            switch (lockLeader) {
                case I_AM_THE_LOCK_LEADER:
                    return ensureLockTableExists();
                case SOMEONE_ELSE_IS_THE_LOCK_LEADER:
                    return waitForSomeoneElseToCreateLockTable();
                default:
                    throw new RuntimeException("We encountered an unknown lock leader status, please contact the AtlasDB team");
            }
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private synchronized TableReference waitForSomeoneElseToCreateLockTable() throws TException {
        while (schemaMutationLockTables.getAllLockTables().isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return getSingleTable();
    }

    private synchronized final TableReference ensureLockTableExists() throws TException {
        Set<TableReference> tables = schemaMutationLockTables.getAllLockTables();

        if (tables.isEmpty()) {
            schemaMutationLockTables.createLockTable(UUID.randomUUID());
        }

        return getSingleTable();
    }

    private TableReference getSingleTable() throws TException {
        Set<TableReference> lockTables = schemaMutationLockTables.getAllLockTables();

        if (lockTables.size() > 1) {
            throw new IllegalStateException(
                    "Multiple schema mutation lock tables have been created.\n" +
                            "This happens when multiple nodes have themselves as lockLeader in the configuration.\n" +
                            "Please ensure the lockLeader is the same for each node, stop all Atlas clients using " +
                            "this keyspace, restart your cassandra cluster and delete all created schema mutation lock tables.\n" +
                            "The tables that clashed were: " + lockTables);
        }

        return Iterables.getOnlyElement(lockTables);
    }

}
