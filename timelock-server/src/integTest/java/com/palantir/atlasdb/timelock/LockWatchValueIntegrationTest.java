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

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.CacheValue;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.keyvalue.api.cache.NoOpTransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionSerializableConflictException;
import com.palantir.atlasdb.util.ByteArrayUtilities;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class LockWatchValueIntegrationTest {
    private static final byte[] DATA_1 = "foo".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_2 = "Caecilius est in horto".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_3 = "canis est in via".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_4 = "Quintus Caecilius Iucundus".getBytes(StandardCharsets.UTF_8);
    private static final Cell CELL_1 =
            Cell.create("bar".getBytes(StandardCharsets.UTF_8), "baz".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_2 =
            Cell.create("bar".getBytes(StandardCharsets.UTF_8), "spam".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_3 =
            Cell.create("eggs".getBytes(StandardCharsets.UTF_8), "baz".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_4 =
            Cell.create("eggs".getBytes(StandardCharsets.UTF_8), "spam".getBytes(StandardCharsets.UTF_8));
    private static final String TABLE = "table";
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);
    private static final CellReference TABLE_CELL_1 = CellReference.of(TABLE_REF, CELL_1);
    private static final CellReference TABLE_CELL_2 = CellReference.of(TABLE_REF, CELL_2);
    private static final CellReference TABLE_CELL_3 = CellReference.of(TABLE_REF, CELL_3);
    private static final CellReference TABLE_CELL_4 = CellReference.of(TABLE_REF, CELL_4);
    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "non-batched timestamp paxos single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.LOCK_WATCH_VALUE_INTEGRATION_TEST,
                    builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)));
    private static final String NAMESPACE =
            String.valueOf(ThreadLocalRandom.current().nextLong());
    private static final byte[] ROW_1 = PtBytes.toBytes("final");
    private static final byte[] ROW_2 = PtBytes.toBytes("destination");
    private static final byte[] ROW_3 = PtBytes.toBytes("awaits");
    private static final ImmutableList<byte[]> ROWS = ImmutableList.of(ROW_1, ROW_2, ROW_3);
    private static final byte[] COL_1 = PtBytes.toBytes("parthenon");
    private static final byte[] COL_2 = PtBytes.toBytes("had");
    private static final byte[] COL_3 = PtBytes.toBytes("columns");
    private static final ImmutableList<byte[]> COLS = ImmutableList.of(COL_1, COL_2, COL_3);

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private TransactionManager txnManager;

    @Before
    public void before() {
        createTransactionManager(0.0);
        LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, TABLE_REF);
    }

    @Test
    public void readOnlyTransactionsDoNotUseCaching() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        txnManager.runTaskReadOnly(txn -> {
            Map<Cell, byte[]> cellMap = txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
            assertThat(cellMap).containsEntry(CELL_1, DATA_1);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });
    }

    @Test
    public void effectivelyReadOnlyTransactionsPublishValuesToCentralCache() {
        putValue();

        Map<Cell, byte[]> result = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_4));
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(
                    txn,
                    ImmutableMap.of(
                            TABLE_CELL_1, CacheValue.of(DATA_1),
                            TABLE_CELL_4, CacheValue.empty()));
            return values;
        });

        disableTable();

        Map<Cell, byte[]> result2 = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_4));
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1, TABLE_CELL_4));
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
            txn.put(TABLE_REF, ImmutableMap.of(CELL_4, DATA_2));
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_4));
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_1, CacheValue.of(DATA_1)));
            return values;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        Map<Cell, byte[]> result2 = txnManager.runTaskThrowOnConflict(txn -> {
            Map<Cell, byte[]> values = txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_4));
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_4, CacheValue.of(DATA_2)));
            return values;
        });

        assertThat(result).containsEntry(CELL_1, DATA_1);
        assertThat(result).containsEntry(CELL_4, DATA_2);
        assertThat(result).containsExactlyInAnyOrderEntriesOf(result2);
    }

    @Test
    public void serializableTransactionsThrowOnCachedReadWriteConflicts() {
        putValue();

        // Nested transactions are normally discouraged, but it is easier to create a conflict this way (compared to
        // using executors, which is much more heavy-handed).
        assertThatThrownBy(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
                    txn.put(TABLE_REF, ImmutableMap.of(CELL_4, DATA_2));

                    txnManager.runTaskThrowOnConflict(txn2 -> {
                        txn2.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_3));
                        return null;
                    });
                    LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);
                    return null;
                }))
                .isExactlyInstanceOf(TransactionSerializableConflictException.class)
                .hasMessageContaining("There was a read-write conflict on table");
    }

    @Test
    public void serializableTransactionsDoNotThrowWhenOverwritingAPreviouslyCachedValue() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        assertThatCode(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
                    txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_3));
                    return null;
                }))
                .doesNotThrowAnyException();
    }

    @Test
    public void serializableTransactionsReadViaTheCacheForConflictChecking() {
        putValue();

        txnManager.runTaskThrowOnConflict(txn -> {
            txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
            txn.put(TABLE_REF, ImmutableMap.of(CELL_4, DATA_2));

            // This is technically corruption, but also confirms that the conflict checking goes through the cache,
            // not the KVS.
            overwriteValueViaKvs(txn, ImmutableMap.of(CELL_1, PtBytes.EMPTY_BYTE_ARRAY));
            return null;
        });
    }

    @Test
    public void putsCauseInvalidationsInSubsequentTransactions() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        txnManager.runTaskThrowOnConflict(txn -> {
            // Read gives the old value due to it being cached; these direct KVS overwrites would be corruption normally
            overwriteValueViaKvs(txn, ImmutableMap.of(CELL_1, DATA_2));
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });

        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_3));
            return null;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_3);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_1, CacheValue.of(DATA_3)));
            return null;
        });
    }

    @Test
    public void leaderElectionFlushesCache() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        CLUSTER.failoverToNewLeader(NAMESPACE);
        LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, TABLE_REF);

        readValueAndAssertLoadedFromRemote();
    }

    @Test
    public void leaderElectionDuringReadOnlyTransactionDoesNotCauseItToFail() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);

            // Calling these finalises the underlying cache, which normally means that no more operations should be
            // carried out on the cache. However, since this cache should be cleared after the election, it is OK to
            // do this earlier
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of());

            CLUSTER.failoverToNewLeader(NAMESPACE);
            LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, TABLE_REF);

            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);
            assertThat(extractTransactionCache(txn)).isExactlyInstanceOf(NoOpTransactionScopedCache.class);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });

        // Confirm nothing was flushed to central cache after election
        readValueAndAssertLoadedFromRemote();

        // Finally, check that the value from above was flushed to the central cache
        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });
    }

    @Test
    public void leaderElectionDuringWriteTransactionCausesTransactionToRetry() {
        putValue();
        readValueAndAssertLoadedFromRemote();

        assertThatThrownBy(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);

                    CLUSTER.failoverToNewLeader(NAMESPACE);
                    LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, TABLE_REF);

                    txn.put(TABLE_REF, ImmutableMap.of(CELL_2, DATA_2));
                    assertHitValues(txn, ImmutableSet.of());
                    return null;
                }))
                .isInstanceOf(TransactionFailedRetriableException.class);

        readValueAndAssertLoadedFromRemote();

        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);

            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1));
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });
    }

    @Test
    public void failedValidationCausesNoOpCacheToBeUsed() {
        createTransactionManager(1.0);

        putValue();
        readValueAndAssertLoadedFromRemote();

        assertThatThrownBy(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    overwriteValueViaKvs(txn, ImmutableMap.of(CELL_1, DATA_2));
                    txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
                    return null;
                }))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Failed lock watch cache validation - will retry without caching");

        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_2);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of());
            return null;
        });
    }

    @Test
    public void getRowsCachedValidatesCorrectly() {
        createTransactionManager(1.0);

        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1, CELL_2, DATA_2, CELL_3, DATA_3, CELL_4, DATA_4));
            return null;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        Set<byte[]> rows = ImmutableSet.of(CELL_1.getRowName(), CELL_3.getRowName());
        ColumnSelection columns =
                ColumnSelection.create(ImmutableSet.of(CELL_1.getColumnName(), CELL_2.getColumnName()));

        NavigableMap<byte[], RowResult<byte[]>> remoteRead = txnManager.runTaskThrowOnConflict(txn -> {
            NavigableMap<byte[], RowResult<byte[]>> read = txn.getRows(TABLE_REF, rows, columns);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(
                    txn,
                    ImmutableMap.of(
                            TABLE_CELL_1,
                            CacheValue.of(DATA_1),
                            TABLE_CELL_2,
                            CacheValue.of(DATA_2),
                            TABLE_CELL_3,
                            CacheValue.of(DATA_3),
                            TABLE_CELL_4,
                            CacheValue.of(DATA_4)));
            return read;
        });

        // New set of rows and columns to force new references (to test every part of the equality checking)
        Set<byte[]> rows2 =
                ImmutableSet.of("bar".getBytes(StandardCharsets.UTF_8), "eggs".getBytes(StandardCharsets.UTF_8));
        ColumnSelection columns2 = ColumnSelection.create(
                ImmutableSet.of("baz".getBytes(StandardCharsets.UTF_8), "spam".getBytes(StandardCharsets.UTF_8)));

        NavigableMap<byte[], RowResult<byte[]>> cacheRead = txnManager.runTaskThrowOnConflict(txn -> {
            NavigableMap<byte[], RowResult<byte[]>> read = txn.getRows(TABLE_REF, rows2, columns2);
            assertHitValues(txn, ImmutableSet.of(TABLE_CELL_1, TABLE_CELL_2, TABLE_CELL_3, TABLE_CELL_4));
            assertLoadedValues(txn, ImmutableMap.of());
            return read;
        });

        assertThat(ByteArrayUtilities.areRowResultsEqual(remoteRead, cacheRead)).isTrue();
    }

    @Test
    public void getRowsUsesCacheAsExpected() {
        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1, CELL_2, DATA_2, CELL_3, DATA_3));
            return null;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        Set<byte[]> rows =
                ImmutableSet.of("bar".getBytes(StandardCharsets.UTF_8), "eggs".getBytes(StandardCharsets.UTF_8));
        ColumnSelection columns = ColumnSelection.create(
                ImmutableSet.of("baz".getBytes(StandardCharsets.UTF_8), "spam".getBytes(StandardCharsets.UTF_8)));

        txnManager.runTaskThrowOnConflict(txn -> {
            NavigableMap<byte[], RowResult<byte[]>> remoteRead = txn.getRows(TABLE_REF, rows, columns);
            txn.delete(TABLE_REF, ImmutableSet.of(CELL_1));
            assertHitValues(txn, ImmutableSet.of());
            // we loaded all 4 values, but since we deleted one of them, it will not be in the digest
            assertLoadedValues(
                    txn,
                    ImmutableMap.of(
                            CellReference.of(TABLE_REF, CELL_2),
                            CacheValue.of(DATA_2),
                            CellReference.of(TABLE_REF, CELL_3),
                            CacheValue.of(DATA_3),
                            TABLE_CELL_4,
                            CacheValue.empty()));
            return remoteRead;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);
        // truncate the table to verify we are really using the cached values
        truncateTable();

        txnManager.runTaskThrowOnConflict(txn -> {
            NavigableMap<byte[], RowResult<byte[]>> read = txn.getRows(TABLE_REF, rows, columns);
            // these values were cached in the previous transaction, so we confirm that they are recorded as hits
            assertHitValues(
                    txn,
                    ImmutableSet.of(
                            CellReference.of(TABLE_REF, CELL_2), CellReference.of(TABLE_REF, CELL_3), TABLE_CELL_4));
            // we weren't able to cache our own write, so we look this up
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_1, CacheValue.empty()));
            return read;
        });
    }

    @Test
    public void nearbyCommitsDoNotAffectResultsPresentInCache() {
        createTransactionManager(1.0);
        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1, CELL_2, DATA_2, CELL_3, DATA_3));
            return null;
        });
        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        LockWatchIntegrationTestUtilities.CommitStageCondition<Void> condition =
                new LockWatchIntegrationTestUtilities.CommitStageCondition<>((startTs, commitTs) -> {
                    LockWatchCache cache = LockWatchIntegrationTestUtilities.extractInternalLockWatchManager(txnManager)
                            .getCache();
                    simulateOverlappingWriteTransaction(cache, startTs, commitTs);
                    return null;
                });

        assertThatCode(() -> txnManager.runTaskWithConditionThrowOnConflict(condition, (txn, _unused) -> {
                    condition.initialiseWithStartTimestamp(txn.getTimestamp());
                    txnManager.getTimestampManagementService().fastForwardTimestamp(txn.getTimestamp() + 1_000_000);

                    txn.get(TABLE_REF, ImmutableSet.of(CELL_1));
                    // A write forces this to go through serializable conflict checking
                    txn.put(TABLE_REF, ImmutableMap.of(CELL_2, DATA_1));

                    return null;
                }))
                .doesNotThrowAnyException();
    }

    @Test
    public void lateAbortingTransactionDoesNotFlushValuesToCentralCache() {
        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1, CELL_2, DATA_2, CELL_3, DATA_3));
            return null;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        LockWatchIntegrationTestUtilities.CommitStageCondition<Void> commitFailingCondition =
                new LockWatchIntegrationTestUtilities.CommitStageCondition<>((_unused1, _unused2) -> {
                    throw new RuntimeException("Transaction failed at commit time");
                });

        assertThatThrownBy(
                        () -> txnManager.runTaskWithConditionThrowOnConflict(commitFailingCondition, (txn, _unused) -> {
                            commitFailingCondition.initialiseWithStartTimestamp(txn.getTimestamp());
                            txn.put(TABLE_REF, ImmutableMap.of(CELL_4, DATA_4));
                            txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2, CELL_3));
                            return null;
                        }))
                .isInstanceOf(RuntimeException.class);

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);

        txnManager.runTaskThrowOnConflict(txn -> {
            // Confirm that the previous transaction did not commit writes
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_4))).isEmpty();
            txn.get(TABLE_REF, ImmutableSet.of(CELL_1, CELL_2, CELL_3));

            assertLoadedValues(
                    txn,
                    ImmutableMap.of(
                            TABLE_CELL_1,
                            CacheValue.of(DATA_1),
                            TABLE_CELL_2,
                            CacheValue.of(DATA_2),
                            TABLE_CELL_3,
                            CacheValue.of(DATA_3),
                            TABLE_CELL_4,
                            CacheValue.empty()));
            assertHitValues(txn, ImmutableSet.of());
            return null;
        });
    }

    @Test
    public void valueStressTest() {
        createTransactionManager(1.0);
        int numThreads = 200;
        int numTransactions = 10_000;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<Future<?>> transactions = IntStream.range(0, numTransactions)
                .mapToObj(_num -> executor.submit(this::randomTransactionTask))
                .collect(Collectors.toList());
        for (Future<?> transaction : transactions) {
            try {
                transaction.get(60, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TransactionFailedRetriableException)) {
                    fail("Encountered nonretriable exception");
                }
            } catch (InterruptedException | TimeoutException e) {
                fail("Transaction took too long", e);
            }
        }

        executor.shutdown();
    }

    private void randomTransactionTask() {
        txnManager.runTaskThrowOnConflict(txn -> {
            while (chance()) {
                txn.put(TABLE_REF, ImmutableMap.of(randomCell(), randomData()));
            }

            while (chance()) {
                txn.delete(TABLE_REF, ImmutableSet.of(randomCell()));
            }

            while (chance()) {
                txn.get(TABLE_REF, ImmutableSet.of(randomCell()));
            }

            while (chance()) {
                txn.getRows(TABLE_REF, randomSelection(ROWS), ColumnSelection.create(randomSelection(COLS)));
            }

            return null;
        });
    }

    private void simulateOverlappingWriteTransaction(
            LockWatchCache cache, long theirStartTimestamp, long theirCommitTimestamp) {
        LockToken lockToken = LockToken.of(UUID.randomUUID());
        LockWatchVersion lastKnownVersion =
                cache.getEventCache().lastKnownVersion().get();

        long ourStartTimestamp = theirStartTimestamp + 1;
        long ourCommitTimestamp = theirCommitTimestamp + 1;
        cache.processStartTransactionsUpdate(
                ImmutableSet.of(ourStartTimestamp), createLockSuccessUpdate(lockToken, lastKnownVersion));
        cache.processCommitTimestampsUpdate(
                ImmutableSet.of(TransactionUpdate.builder()
                        .startTs(ourStartTimestamp)
                        .commitTs(ourCommitTimestamp)
                        .writesToken(lockToken)
                        .build()),
                createUnlockSuccessUpdate(lastKnownVersion));

        txnManager.getKeyValueService().put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_4), theirCommitTimestamp - 1);
        txnManager.getTransactionService().putUnlessExists(theirCommitTimestamp - 1, theirCommitTimestamp + 1);
    }

    private void overwriteValueViaKvs(Transaction transaction, Map<Cell, byte[]> values) {
        txnManager.getKeyValueService().put(TABLE_REF, values, transaction.getTimestamp() - 2);
        txnManager
                .getTransactionService()
                .putUnlessExists(transaction.getTimestamp() - 2, transaction.getTimestamp() - 1);
    }

    private void readValueAndAssertLoadedFromRemote() {
        txnManager.runTaskThrowOnConflict(txn -> {
            assertThat(txn.get(TABLE_REF, ImmutableSet.of(CELL_1))).containsEntry(CELL_1, DATA_1);
            assertHitValues(txn, ImmutableSet.of());
            assertLoadedValues(txn, ImmutableMap.of(TABLE_CELL_1, CacheValue.of(DATA_1)));
            return null;
        });
    }

    private void putValue() {
        txnManager.runTaskThrowOnConflict(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));
            return null;
        });

        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);
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

    private void truncateTable() {
        txnManager.getKeyValueService().truncateTable(TABLE_REF);
    }

    private LockWatchValueScopingCache extractValueCache() {
        return (LockWatchValueScopingCache)
                LockWatchIntegrationTestUtilities.extractInternalLockWatchManager(txnManager)
                        .getCache()
                        .getValueCache();
    }

    private TransactionScopedCache extractTransactionCache(Transaction txn) {
        return extractValueCache().getTransactionScopedCache(txn.getTimestamp());
    }

    private void createTransactionManager(double validationProbability) {
        txnManager =
                LockWatchIntegrationTestUtilities.createTransactionManager(validationProbability, CLUSTER, NAMESPACE);
    }

    private static Set<byte[]> randomSelection(ImmutableList<byte[]> rows) {
        return rows.stream().filter(_unused -> chance()).collect(Collectors.toSet());
    }

    private static Cell randomCell() {
        return Cell.create(ROWS.get(randomIndex()), COLS.get(randomIndex()));
    }

    private static int randomIndex() {
        return ThreadLocalRandom.current().nextInt(0, ROWS.size());
    }

    private static boolean chance() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    private static byte[] randomData() {
        return PtBytes.toBytes(ThreadLocalRandom.current().nextLong(0, 1_000_000));
    }

    private static LockWatchStateUpdate.Success createUnlockSuccessUpdate(LockWatchVersion lastKnownVersion) {
        return LockWatchStateUpdate.success(
                lastKnownVersion.id(),
                lastKnownVersion.version() + 2,
                ImmutableList.of(UnlockEvent.builder(ImmutableSet.of(AtlasCellLockDescriptor.of(
                                TABLE_REF.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName())))
                        .build(lastKnownVersion.version() + 2)));
    }

    private static LockWatchStateUpdate.Success createLockSuccessUpdate(
            LockToken lockToken, LockWatchVersion lastKnownVersion) {
        return LockWatchStateUpdate.success(
                lastKnownVersion.id(),
                lastKnownVersion.version() + 1,
                ImmutableList.of(LockEvent.builder(
                                ImmutableSet.of(AtlasCellLockDescriptor.of(
                                        TABLE_REF.getQualifiedName(), CELL_1.getRowName(), CELL_1.getColumnName())),
                                lockToken)
                        .build(lastKnownVersion.version() + 1)));
    }
}
