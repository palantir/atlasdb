/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.config.LockLeader;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class UniqueSchemaMutationLockTable {
    private static final Logger log = LoggerFactory.getLogger(UniqueSchemaMutationLockTable.class);
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
                    throw new RuntimeException(
                            "We encountered an unknown lock leader status, please contact the AtlasDB team");
            }
        } catch (TException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    @SuppressFBWarnings("SWL_SLEEP_WITH_LOCK_HELD")
    private synchronized TableReference waitForSomeoneElseToCreateLockTable() throws TException {
        while (schemaMutationLockTables.getAllLockTables().isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for lock table to be created", e);
            }
        }

        return getSingleTable();
    }

    private synchronized TableReference ensureLockTableExists() throws TException {
        Set<TableReference> tables = schemaMutationLockTables.getAllLockTables();

        if (tables.isEmpty()) {
            schemaMutationLockTables.createLockTable();
        }

        return getSingleTable();
    }

    private TableReference getSingleTable() throws TException {
        Set<TableReference> lockTables = schemaMutationLockTables.getAllLockTables();

        if (lockTables.size() > 1) {
            throw new IllegalStateException("Multiple schema mutation lock tables have been created.\n"
                    + "This happens when multiple nodes have themselves as lockLeader in the configuration.\n"
                    + "Please ensure the lockLeader is the same for each node, stop all Atlas clients using this "
                    + "keyspace, restart your cassandra cluster and run the clean-cass-locks-state cli command.\n"
                    + "The tables that clashed were: " + lockTables);
        }

        return Iterables.getOnlyElement(lockTables);
    }
}
