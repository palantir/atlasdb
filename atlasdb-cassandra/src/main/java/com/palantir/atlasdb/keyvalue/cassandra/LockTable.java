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
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return create(new LockTableLeaderElector(), new CassandraDataStore(config, clientPool));
    }

    public static LockTable create(LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
        TableReference electedTable = new LockTableCreator(leaderElector, cassandraDataStore).create();
        return new LockTable(electedTable);
    }

    private static class LockTableCreator {
        private static final Logger log = LoggerFactory.getLogger(LockTableCreator.class);

        private final LockTableLeaderElector leaderElector;
        private CassandraDataStore cassandraDataStore;

        public LockTableCreator(LockTableLeaderElector leaderElector, CassandraDataStore cassandraDataStore) {
            this.leaderElector = leaderElector;
            this.cassandraDataStore = cassandraDataStore;
        }

        public TableReference create() {
            TableReference winnerTable = createElectedTableIfNotExists();
            removeLosers();
            return winnerTable;
        }

        private TableReference createElectedTableIfNotExists() {
            // Return early if we already agreed on a lock table
            Optional<TableReference> currentLockTable = getCurrentLockTable();
            if (currentLockTable.isPresent()) {
                return currentLockTable.get();
            }

            TableReference ourLockTable = createPossibleLockTable();

            TableReference winnerTable = leaderElector.proposeTableToBeTheCorrectOne(ourLockTable);
            markAsWinner(winnerTable);
            return winnerTable;
        }

        private Optional<TableReference> getCurrentLockTable() {
            try {
                return cassandraDataStore.allTables().stream()
                        .filter(possibleLockTables())
                        .filter(elected())
                        .findAny();
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        private Predicate<TableReference> elected() {
            return (tableReference) -> {
                try {
                    return cassandraDataStore.valueExists(tableReference, "elected", "elected", "elected");
                } catch (Exception e) {
                    return false;
                }
            };
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

        private void removeLosers() {
            try {
                cassandraDataStore.allTables().stream()
                        .filter(possibleLockTables())
                        .filter(elected().negate())
                        .forEach(this::removeTable);
            } catch (Exception e) {
                log.warn("Failed to clean up non-elected locks tables. The cluster should still run normally.", e);
                throw new RuntimeException(e);
            }
        }

        private void removeTable(TableReference tableReference) {
            try {
                cassandraDataStore.removeTable(tableReference);
            } catch (Exception e) {
                log.warn(String.format("Failed to remove non-elected locks table %s.", tableReference.getTablename()), e);
            }
        }
    }

    private static Predicate<TableReference> possibleLockTables() {
        return tableReference -> tableReference.getTablename().startsWith("_locks");
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
