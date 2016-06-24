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

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class LockTable {
    public final TableReference lockTable;

    public LockTable(TableReference lockTable) {
        this.lockTable = lockTable;
    }

    public static LockTable create(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        return create(clientPool, new LockTableLeaderElector(), new CassandraDataStore(config, clientPool));
    }

    public static LockTable create(CassandraClientPool clientPool, LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
        TableReference electedTable = new LockTableCreator(leaderElector, cassandraDataStore).create();
        return new LockTable(electedTable);
    }

    private static class LockTableCreator {
        private final LockTableLeaderElector leaderElector;
        private CassandraDataStore cassandraDataStore;

        public LockTableCreator(LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
            this.leaderElector = leaderElector;
            this.cassandraDataStore = cassandraDataStore;
        }

        public TableReference create() {
            // Check if ANY lock table exists already
            // if so, return the name
            Optional<TableReference> currentLockTable = getCurrentLockTable();
            if (currentLockTable.isPresent()) {
                return currentLockTable.get();
            }

            TableReference ourLockTable = createPossibleLockTable();

            TableReference winnerTable = leaderElector.proposeTableToBeTheCorrectOne(ourLockTable);
            markAsWinner(winnerTable);
    /*
            removeLosers(winnerTableName);
     */
            return winnerTable;
        }

        private Optional<TableReference> getCurrentLockTable() {
            return Optional.empty();
        }

        private TableReference createPossibleLockTable() {
            try {
                TableReference candidateTable = TableReference.createWithEmptyNamespace("_locks");
                cassandraDataStore.createTable(candidateTable);
                return candidateTable;
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        private void markAsWinner(TableReference winnerTable) {
            String elected = "elected";
            cassandraDataStore.put(winnerTable, elected, elected, elected);
        }
    }

    public TableReference getLockTable() {
        return lockTable;
    }

    /**
     * This returns both active and inactive lock tables.
     */
    public Set<TableReference> getAllLockTables() {
        return ImmutableSet.of(lockTable);
    }
}
