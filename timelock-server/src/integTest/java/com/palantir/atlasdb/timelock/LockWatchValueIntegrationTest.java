/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.AbstractAsyncTimelockServiceIntegrationTest.DEFAULT_SINGLE_SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.lock.watch.LockWatchVersion;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class LockWatchValueIntegrationTest {
    private static final String TEST_PACKAGE = "package";
    private static final byte[] DATA_1 = "foo".getBytes();
    private static final byte[] DATA_2 = "Caecilius est in horto".getBytes();
    private static final byte[] DATA_3 = "canis est in via".getBytes();
    private static final Cell CELL_1 = Cell.create("bar".getBytes(), "baz".getBytes());
    private static final Cell CELL_2 = Cell.create("eggs".getBytes(), "spam".getBytes());
    private static final String TABLE = "table";
    private static final String TABLE_2 = "vesuvius";
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);
    private static final TableReference TABLE_REF_2 = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);
    private static final CellReference TABLE_CELL_1 = CellReference.of(TABLE_REF, CELL_1);
    private static final CellReference TABLE_CELL_2 = CellReference.of(TABLE_REF, CELL_2);
    private static final TestableTimelockCluster CLUSTER =
            new TestableTimelockCluster("paxosSingleServer.ftl", DEFAULT_SINGLE_SERVER);

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private TransactionManager txnManager;

    @Before
    public void before() {
        txnManager = createTransactionManager(0.0);
    }

    @Test
    public void effectivelyReadOnlyTransactionsPublishValuesToCentralCache() {
        putValue();

        Map<Cell, byte[]> result = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2));
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(
                    txn,
                    ImmutableMap.of(
                            TABLE_CELL_1, CacheValue.of(DATA_1),
                            TABLE_CELL_2, CacheValue.empty()));
            return values;
        });

        disableTable();

        Map<Cell, byte[]> result2 = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2));
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1, TABLE_CELL_2));
            assertLoadedValues(txn, ImmutableMap.of());
            return values;
        });

        assertThat(result).containsEntry(CELL_1, DATA_1);
        assertThat(result).containsExactlyInAnyOrderEntriesOf(result2);
    }

    @Test
    public void readWriteTransactionsPublishValuesToCentralCache() {
        putValue();

        Map<Cell, byte[]> result = txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_2, DATA_2));
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2));
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_1, CacheValue.of(DATA_1)));
            return values;
        });

        awaitUnlock();

        Map<Cell, byte[]> result2 = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2));
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_2, CacheValue.of(DATA_2)));
            return values;
        });

        assertThat(result).containsEntry(CELL_1, DATA_1);
        assertThat(result).containsEntry(CELL_2, DATA_2);
        assertThat(result).containsExactlyInAnyOrderEntriesOf(result2);
    }

    @Test
    public void serializableTransactionsThrowOnCachedReadWriteConflicts() {
        putValue();

        // Nested transactions are normally discouraged, but it is easier to create a conflict this way (compared to
        // using executors, which is much more heavy-handed).
        assertThatThrownBy(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
                    txn.put(TABLE_REF, ImmutableMap.of(CELL_2, DATA_2));

                    txnManager.runTaskThrowOnConflict(txn2 -> {
                        txn2.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_3));
                        return null;
                    });
                    awaitUnlock();
                    return null;
                }))
                .isExactlyInstanceOf(TransactionSerializableConflictException.class)
                .hasMessageContaining("There was a read-write conflict on table");
    }

    private void putValue() {
        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));
            return null;
        });

        awaitUnlock();
    }

    private void assertHitValues(Transaction transaction, Set<CellReference> expectedCells) {
        TransactionScopedCache cache = extractTransactionCache(transaction);
        cache.finalise();
        assertThat(cache.getHitDigest().hitCells()).containsExactlyInAnyOrderElementsOf(expectedCells);
    }

    private void assertLoadedValues(Transaction transaction, Map<CellReference, CacheValue> expectedValues) {
        TransactionScopedCache cache = extractTransactionCache(transaction);
        cache.finalise();
        assertThat(cache.getValueDigest().loadedValues()).containsExactlyInAnyOrderEntriesOf(expectedValues);
    }

    /**
     * Drops the table directly via the key value service. In general, this operation *would* cause data corruption
     * when used in conjunction with caching, as values in the cache would still be valid (Timelock does not know
     * that the table has been dropped, and thus the cache believes that values are still legitimate). However, using
     * this here allows us to determine whether the values have been read from the KVS or not.
     */
    private void disableTable() {
        txnManager.getKeyValueService().dropTable(TABLE_REF);
    }

    /**
     * Lock watch events tend to come in pairs - a lock and an unlock event. However, unlocks are asynchronous, and
     * thus we need to wait until we have received the unlock event before proceeding for deterministic testing
     * behaviour.
     */
    private void awaitUnlock() {
        LockWatchManagerInternal lockWatchManager = extractInternalLockWatchManager();
        Awaitility.await("Unlock event recieved")
                .atMost(Duration.ofSeconds(5))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> {
                    // Empty transaction will still get an update for lock watches
                    txnManager.runTaskThrowOnConflict(txn -> null);
                    return lockWatchManager
                            .getCache()
                            .getEventCache()
                            .lastKnownVersion()
                            .map(LockWatchVersion::version)
                            .filter(v -> (v % 2) == 0)
                            .isPresent();
                });
    }

    private LockWatchManagerInternal extractInternalLockWatchManager() {
        return (LockWatchManagerInternal) txnManager.getLockWatchManager();
    }

    private LockWatchValueScopingCache extractValueCache() {
        return (LockWatchValueScopingCache)
                extractInternalLockWatchManager().getCache().getValueCache();
    }

    private TransactionScopedCache extractTransactionCache(Transaction txn) {
        return extractValueCache().getOrCreateTransactionScopedCache(txn.getTimestamp());
    }

    private static TransactionManager createTransactionManager(double validationProbability) {
        return TimeLockTestUtils.createTransactionManager(
                        CLUSTER,
                        UUID.randomUUID().toString(),
                        AtlasDbRuntimeConfig.defaultRuntimeConfig(),
                        ImmutableAtlasDbConfig.builder()
                                .lockWatchCaching(LockWatchCachingConfig.builder()
                                        .validationProbability(validationProbability)
                                        .build()),
                        Optional.empty(),
                        createSchema())
                .transactionManager();
    }

    private static Schema createSchema() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
        TableDefinition tableDef = new TableDefinition() {
            {
                rowName();
                rowComponent("key", ValueType.BLOB);
                noColumns();
                enableCaching();
                conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
            }
        };
        schema.addTableDefinition(TABLE, tableDef);
        return schema;
    }
}
