package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class UniqueSchemaMutationLockTable {
    private final SchemaMutationLockTables schemaMutationLockTables;


    public UniqueSchemaMutationLockTable(SchemaMutationLockTables schemaMutationLockTables) {
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
                return schemaMutationLockTables.createLockTable(UUID.randomUUID());
            }

            return Iterables.getOnlyElement(tables);
    }
}
