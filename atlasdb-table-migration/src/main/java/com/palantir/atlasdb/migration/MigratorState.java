/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.concurrent.PTExecutors;

public class MigratorState {
    private final MigrationCoordinationService coordinationService;
    private final ProgressCheckPoint checkpointer;
    private final TransactionManager txManager;

    public MigratorState(MigrationCoordinationService coordinationService, ExecutorService executor,
            ProgressCheckPoint checkpointer, TransactionManager txManager, Set<TableReference> tablesToMigrate) {
        this.coordinationService = coordinationService;
        this.checkpointer = checkpointer;
        this.txManager = txManager;
        executor.submit(() -> tablesToMigrate.forEach(this::moveState));
    }

    public static MigratorState createAndRun(TransactionManager manager, MigrationCoordinationService coordination,
            Set<TableReference> tablesToMigrate) {
        return new MigratorState(
                coordination,
                PTExecutors.newSingleThreadExecutor(true),
                null,
                manager,
                tablesToMigrate);
    }

    public void runWithRetry(Supplier<Boolean> method) throws InterruptedException {
        boolean success = method.get();
        while (!success) {
            Thread.sleep(10_000);
            method.get();
        }
    }

    private TableReference getMigrationTable(TableReference tableRef) {
        return TableReference.createFromFullyQualifiedName(tableRef.toString() + ".migration");
    }

    private void moveState(TableReference startTable) {
        try {
            switch (getState(startTable)) {
                case WRITE_FIRST_ONLY:
                    new InitialState().start(startTable);
                    return;
                case WRITE_BOTH_READ_FIRST:
                    new Migration().start(startTable);
                    return;
                case WRITE_BOTH_READ_SECOND:
                    new EndMigration().start(startTable);
                case WRITE_SECOND_READ_SECOND:
                    return;
                default:
                    throw new IllegalStateException("Unknown migration state");
            }
        } catch (InterruptedException e) {
            //deal with this
        }
    }

    private MigrationState getState(TableReference startTable) {
        return coordinationService.getMigrationState(startTable, txManager.getImmutableTimestamp()).migrationsState();
    }

    private interface State {
        void start(TableReference startTable) throws InterruptedException;
    }

    public class InitialState implements State {
        @Override
        public void start(TableReference startTable) throws InterruptedException {
            runWithRetry(() -> coordinationService.startMigration(startTable, getMigrationTable(startTable)));
            runWithRetry(() -> getState(startTable) != MigrationState.WRITE_FIRST_ONLY);
            moveState(startTable);
        }
    }

    public class Migration implements State {
        @Override
        public void start(TableReference startTable) throws InterruptedException {
            TableReference targetTable = getMigrationTable(startTable);
            new LiveMigrator(txManager, startTable, targetTable, checkpointer).runMigration();
            runWithRetry(() -> coordinationService.endMigration(startTable));
            runWithRetry(() -> getState(startTable) != MigrationState.WRITE_BOTH_READ_FIRST);
            moveState(startTable);
        }
    }

    public class EndMigration implements State {
        @Override
        public void start(TableReference startTable) throws InterruptedException {
            runWithRetry(() -> coordinationService.endDualWrite(startTable));
            runWithRetry(() -> getState(startTable) != MigrationState.WRITE_BOTH_READ_SECOND);
            moveState(startTable);
        }
    }
}
