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

    public LockTable(String ref) {
        this.lockTable = TableReference.createWithEmptyNamespace(ref);
    }

    public static LockTable create(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        return create(clientPool, new LockTableLeaderElector(), new CassandraDataStore(config, clientPool));
    }

    public static LockTable create(CassandraClientPool clientPool, LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
        String ref = new LockTableCreator(leaderElector, cassandraDataStore).create();
        return new LockTable(ref);
    }

    private static class LockTableCreator {
        private final LockTableLeaderElector leaderElector;
        private CassandraDataStore cassandraDataStore;

        public LockTableCreator(LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
            this.leaderElector = leaderElector;
            this.cassandraDataStore = cassandraDataStore;
        }

        public String create() {
            // Check if ANY lock table exists already
            // if so, return the name
            Optional<String> currentLockTableName = getCurrentLockTableName();
            if (currentLockTableName.isPresent()) {
                return currentLockTableName.get();
            }

            String ourLockTableName = createPossibleLockTable();

            String winnerTableName = leaderElector.proposeTableToBeTheCorrectOne(ourLockTableName);
            markAsWinner(winnerTableName);
    /*
            removeLosers(winnerTableName);
     */
            return winnerTableName;
        }

        private Optional<String> getCurrentLockTableName() {
            return Optional.empty();
        }

        private String createPossibleLockTable() {
            try {
                String tableName = "_locks";
                cassandraDataStore.createTable(tableName);
                return tableName;
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        private void markAsWinner(String winnerTableName) {
            String elected = "elected";
            cassandraDataStore.put(winnerTableName, elected, elected, elected);
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
