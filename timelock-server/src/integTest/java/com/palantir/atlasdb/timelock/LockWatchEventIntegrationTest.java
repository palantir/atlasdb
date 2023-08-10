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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.atlasdb.transaction.api.ValueAndChangeMetadata;
import com.palantir.atlasdb.transaction.impl.PreCommitConditions;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.AtlasCellLockDescriptor;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.ImmutableLockRequest;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class LockWatchEventIntegrationTest {
    private static final byte[] DATA_1 = "snooping".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_2 = "as".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_3 = "usual".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA_4 = "I see".getBytes(StandardCharsets.UTF_8);
    private static final ChangeMetadata CREATE_CHANGE_METADATA = ChangeMetadata.created(DATA_1);
    private static final ChangeMetadata DELETE_CHANGE_METADATA = ChangeMetadata.deleted(DATA_2);
    private static final ChangeMetadata UNCHANGED_CHANGE_METADATA = ChangeMetadata.unchanged();
    private static final ChangeMetadata UPDATE_CHANGE_METADATA = ChangeMetadata.updated(DATA_3, DATA_4);
    private static final Cell CELL_1 =
            Cell.create("never".getBytes(StandardCharsets.UTF_8), "gonna".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_2 =
            Cell.create("never".getBytes(StandardCharsets.UTF_8), "give".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_3 =
            Cell.create("you".getBytes(StandardCharsets.UTF_8), "gonna".getBytes(StandardCharsets.UTF_8));
    private static final Cell CELL_4 =
            Cell.create("you".getBytes(StandardCharsets.UTF_8), "give".getBytes(StandardCharsets.UTF_8));
    private static final byte[] ROW = "up".getBytes(StandardCharsets.UTF_8);
    private static final String TABLE = LockWatchIntegrationTestUtilities.TABLE;
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);
    private static final TableReference TABLE_2_REF =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, LockWatchIntegrationTestUtilities.TABLE_2);
    private static final TableReference SERIALIZABLE_WATCHED_TABLE_REF =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, LockWatchIntegrationTestUtilities.SERIALIZABLE_TABLE);
    private static final String NAMESPACE =
            String.valueOf(ThreadLocalRandom.current().nextLong());
    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "non-batched timestamp paxos single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.LOCK_WATCH_EVENT_INTEGRATION_TEST,
                    builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                            .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)));

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private TransactionManager txnManager;

    @Before
    public void setUpAndAwaitTableWatched() {
        txnManager = LockWatchIntegrationTestUtilities.createTransactionManager(0.0, CLUSTER, NAMESPACE);
        LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, TABLE_REF);

        // Register watch for table manually since we cannot enable caching for it as it uses the SERIALIZABLE
        // conflict handler
        LockWatchReference entireTableReference =
                LockWatchReferences.entireTable(SERIALIZABLE_WATCHED_TABLE_REF.getQualifiedName());
        txnManager.getLockWatchManager().registerPreciselyWatches(ImmutableSet.of(entireTableReference));
        LockWatchIntegrationTestUtilities.awaitTableWatched(txnManager, SERIALIZABLE_WATCHED_TABLE_REF);
    }

    @Test
    public void exactRowWatchesCanBeRegistered() {
        LockWatchReference exactRowReference = LockWatchReferences.exactRow(TABLE_2_REF.getQualifiedName(), ROW);
        txnManager.getLockWatchManager().registerPreciselyWatches(ImmutableSet.of(exactRowReference));
        LockWatchIntegrationTestUtilities.awaitLockWatchCreated(txnManager, exactRowReference);
    }

    @Test
    public void commitUpdatesDoNotContainTheirOwnCommitLocks() {
        CommitUpdateExtractingCondition firstCondition = new CommitUpdateExtractingCondition();
        CommitUpdateExtractingCondition secondCondition = new CommitUpdateExtractingCondition();

        txnManager.runTaskWithConditionThrowOnConflict(firstCondition, (outerTxn, _unused1) -> {
            firstCondition.initialiseWithStartTimestamp(outerTxn.getTimestamp());
            outerTxn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));
            txnManager.runTaskWithConditionThrowOnConflict(secondCondition, (innerTxn, _unused2) -> {
                secondCondition.initialiseWithStartTimestamp(innerTxn.getTimestamp());
                innerTxn.put(TABLE_REF, ImmutableMap.of(CELL_2, DATA_2));
                return null;
            });
            return null;
        });

        CommitUpdate firstUpdate = firstCondition.getCommitStageResult();
        CommitUpdate secondUpdate = secondCondition.getCommitStageResult();

        assertThat(extractDescriptorsFromUpdate(firstUpdate))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(CELL_2));
        assertThat(extractDescriptorsFromUpdate(secondUpdate)).isEmpty();
    }

    @Test
    public void multipleTransactionVersionsReturnsSnapshotAndOnlyRelevantRecentEvents() {
        LockWatchVersion baseVersion = getCurrentVersion();

        performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_1, DATA_1, CELL_2, DATA_2));

        OpenTransaction secondTxn = startSingleTransaction();

        performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_3, DATA_3));

        // The purpose of this transaction is to test when we can guarantee that there are some locks taken out
        // without the subsequent unlock event.
        Runnable cleanup = performWriteTransactionThatBlocksAfterLockingCells();

        OpenTransaction fifthTxn = startSingleTransaction();
        TransactionsLockWatchUpdate update = getUpdateForTransactions(Optional.empty(), secondTxn, fifthTxn);

        /*
        There are five transactions in this test, with the following events:

        Transaction 1: lock C1, C2; unlock C1, C2.
        Transaction 2: uncommitted, no events
        Transaction 3: lock C3; unlock C3
        Transaction 4: lock C1, C4; no unlocks (as stuck in commit stage)
        Transaction 5: uncommitted, no events

        From the above, Transactions 1 and 3 should increment the version by 2 each; Transaction 4 by 1. Thus,
        Transaction 2 should be at base + 2, and Transaction 5 at base + 5.
         */
        assertThat(update.clearCache()).isTrue();
        assertThat(update.startTsToSequence().get(secondTxn.getTimestamp()).version())
                .isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(fifthTxn.getTimestamp()).version())
                .isEqualTo(baseVersion.version() + 5);

        assertThat(lockedDescriptors(update.events()))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(CELL_1, CELL_3, CELL_4));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(CELL_3));
        assertThat(watchDescriptors(update.events())).isEmpty();

        secondTxn.finish(_unused -> null);
        fifthTxn.finish(_unused -> null);
        cleanup.run();
    }

    @Test
    public void upToDateVersionReturnsOnlyNecessaryEvents() {
        LockWatchVersion baseVersion = getCurrentVersion();

        performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_1, DATA_1));

        OpenTransaction secondTxn = startSingleTransaction();

        performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_2, DATA_2));

        LockWatchVersion currentVersion = getCurrentVersion();
        performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_3, DATA_3));

        OpenTransaction fifthTxn = startSingleTransaction();

        TransactionsLockWatchUpdate update = getUpdateForTransactions(Optional.of(currentVersion), secondTxn, fifthTxn);

        assertThat(update.clearCache()).isFalse();
        assertThat(update.startTsToSequence().get(secondTxn.getTimestamp()).version())
                .isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(fifthTxn.getTimestamp()).version())
                .isEqualTo(currentVersion.version() + 2);

        // Note that the lock/unlock events for CELL_2 are not present because a more up-to-date version was passed in
        assertThat(lockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(CELL_3));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(CELL_3));
        assertThat(watchDescriptors(update.events())).isEmpty();
    }

    @Test
    public void eventsGeneratedForRowLevelLockWatches() {
        LockWatchReference exactRowReference = LockWatchReferences.exactRow(TABLE_2_REF.getQualifiedName(), ROW);
        txnManager.getLockWatchManager().registerPreciselyWatches(ImmutableSet.of(exactRowReference));
        LockWatchIntegrationTestUtilities.awaitLockWatchCreated(txnManager, exactRowReference);

        OpenTransaction firstTxn = startSingleTransaction();
        LockWatchVersion currentVersion = getCurrentVersion();

        Cell cell = Cell.create(ROW, "down".getBytes(StandardCharsets.UTF_8));
        performWriteMetadataTransactionLockingAndUnlockingCells(
                TABLE_2_REF, ImmutableMap.of(cell, ValueAndChangeMetadata.of(DATA_1, CREATE_CHANGE_METADATA)));

        OpenTransaction thirdTxn = startSingleTransaction();

        TransactionsLockWatchUpdate update = getUpdateForTransactions(Optional.of(currentVersion), firstTxn, thirdTxn);

        assertThat(getAllDescriptorsFromLockWatchEvent(update.events(), LockEventVisitor.INSTANCE))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(TABLE_2_REF, cell));
        assertThat(getAllDescriptorsFromLockWatchEvent(update.events(), UnlockEventVisitor.INSTANCE))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(TABLE_2_REF, cell));
        assertThat(getAllDescriptorsFromLockWatchEvent(update.events(), WatchEventVisitor.INSTANCE))
                .isEmpty();
        assertThat(LockWatchIntegrationTestUtilities.extractMetadata(update.events()))
                .containsExactly(Optional.of(LockRequestMetadata.of(
                        ImmutableMap.of(getDescriptor(TABLE_2_REF, cell), CREATE_CHANGE_METADATA))));
    }

    @Test
    public void leaderElectionDuringTransactionCausesTransactionToFailRetriably() {
        assertThatThrownBy(() -> txnManager.runTaskThrowOnConflict(txn -> {
                    txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));
                    CLUSTER.failoverToNewLeader(NAMESPACE);
                    return null;
                }))
                .isInstanceOf(TransactionFailedRetriableException.class)
                .isExactlyInstanceOf(TransactionLockTimeoutException.class)
                .hasMessageContaining("Locks acquired as part of the transaction protocol are no longer valid.");

        assertThatCode(() -> performWriteTransactionLockingAndUnlockingCells(ImmutableMap.of(CELL_1, DATA_1)))
                .doesNotThrowAnyException();
    }

    @Test
    public void absentMetadataIsVisibleToTransaction() {
        LockRequest requestWithoutMetadata = createTestLockRequest(
                ImmutableSet.of(AtlasRowLockDescriptor.of(TABLE_REF.getQualifiedName(), ROW)), Optional.empty());
        assertThat(lockAndGetEventMetadataFromLockWatchLog(requestWithoutMetadata))
                .isEmpty();
    }

    @Test
    public void nonEmptyMetadataIsVisibleToTransactionForWatchedTable() {
        LockDescriptor lock1 = getDescriptor(TABLE_REF, CELL_1);
        LockDescriptor lock2 = getDescriptor(TABLE_REF, CELL_2);
        LockDescriptor lock3 = getDescriptor(TABLE_REF, CELL_3);
        LockDescriptor lock4 = getDescriptor(TABLE_REF, CELL_4);
        LockRequestMetadata metadata = LockRequestMetadata.of(ImmutableMap.of(
                lock1, CREATE_CHANGE_METADATA,
                lock2, DELETE_CHANGE_METADATA,
                lock3, UNCHANGED_CHANGE_METADATA,
                lock4, UPDATE_CHANGE_METADATA));
        LockRequest requestWithMetadata =
                createTestLockRequest(ImmutableSet.of(lock1, lock2, lock3, lock4), Optional.of(metadata));
        assertThat(lockAndGetEventMetadataFromLockWatchLog(requestWithMetadata)).contains(metadata);
    }

    @Test
    public void transactionUpdateIncludesMetadataFromPutWithMetadata() {
        LockWatchVersion currentVersion = getCurrentVersion();

        performWriteMetadataTransactionLockingAndUnlockingCells(
                TABLE_REF,
                ImmutableMap.of(
                        CELL_1,
                        ValueAndChangeMetadata.of(DATA_1, CREATE_CHANGE_METADATA),
                        CELL_2,
                        ValueAndChangeMetadata.of(DATA_2, DELETE_CHANGE_METADATA)));
        performWriteTransactionLockingAndUnlockingCells(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));

        // Would throw if cells were in the same row due to SERIALIZABLE conflict handler
        Cell cell = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col"));
        Cell cellInOtherRow = Cell.create(PtBytes.toBytes("otherRow"), PtBytes.toBytes("col"));
        performWriteMetadataTransactionLockingAndUnlockingCells(
                SERIALIZABLE_WATCHED_TABLE_REF,
                ImmutableMap.of(
                        cell,
                        ValueAndChangeMetadata.of(DATA_3, UNCHANGED_CHANGE_METADATA),
                        cellInOtherRow,
                        ValueAndChangeMetadata.of(DATA_4, UPDATE_CHANGE_METADATA)));

        TransactionsLockWatchUpdate update =
                getUpdateForTransactions(Optional.of(currentVersion), startSingleTransaction());

        assertThat(LockWatchIntegrationTestUtilities.extractMetadata(update.events()))
                .containsExactly(
                        // From the first txn
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                getDescriptor(TABLE_REF, CELL_1),
                                CREATE_CHANGE_METADATA,
                                getDescriptor(TABLE_REF, CELL_2),
                                DELETE_CHANGE_METADATA))),
                        // From the second txn
                        Optional.empty(),
                        // From the third txn
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                getDescriptor(SERIALIZABLE_WATCHED_TABLE_REF, cell),
                                UNCHANGED_CHANGE_METADATA,
                                getDescriptor(SERIALIZABLE_WATCHED_TABLE_REF, cellInOtherRow),
                                UPDATE_CHANGE_METADATA))));
    }

    @Test
    public void transactionUpdateIncludesMetadataFromDeleteWithMetadata() {
        LockWatchVersion currentVersion = getCurrentVersion();

        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.deleteWithMetadata(TABLE_REF, ImmutableMap.of(CELL_1, CREATE_CHANGE_METADATA));
            return null;
        });
        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.delete(TABLE_REF, ImmutableSet.of(CELL_2));
            return null;
        });

        TransactionsLockWatchUpdate update =
                getUpdateForTransactions(Optional.of(currentVersion), startSingleTransaction());

        assertThat(LockWatchIntegrationTestUtilities.extractMetadata(update.events()))
                .containsExactly(
                        // From the first txn
                        Optional.of(LockRequestMetadata.of(
                                ImmutableMap.of(getDescriptor(TABLE_REF, CELL_1), CREATE_CHANGE_METADATA))),
                        // From the second txn
                        Optional.empty());
    }

    @Test
    public void metadataFilteredAccordingToWatches() {
        LockWatchVersion currentVersion = getCurrentVersion();

        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.putWithMetadata(
                    TABLE_REF, ImmutableMap.of(CELL_1, ValueAndChangeMetadata.of(DATA_1, CREATE_CHANGE_METADATA)));
            txn.putWithMetadata(
                    TABLE_2_REF, ImmutableMap.of(CELL_2, ValueAndChangeMetadata.of(DATA_2, DELETE_CHANGE_METADATA)));
            return null;
        });
        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1));
            txn.putWithMetadata(
                    TABLE_2_REF, ImmutableMap.of(CELL_2, ValueAndChangeMetadata.of(DATA_2, DELETE_CHANGE_METADATA)));
            return null;
        });
        performWriteMetadataTransactionLockingAndUnlockingCells(
                TABLE_2_REF, ImmutableMap.of(CELL_3, ValueAndChangeMetadata.of(DATA_3, UNCHANGED_CHANGE_METADATA)));
        performWriteMetadataTransactionLockingAndUnlockingCells(
                SERIALIZABLE_WATCHED_TABLE_REF,
                ImmutableMap.of(CELL_4, ValueAndChangeMetadata.of(DATA_3, UPDATE_CHANGE_METADATA)));

        TransactionsLockWatchUpdate update =
                getUpdateForTransactions(Optional.of(currentVersion), startSingleTransaction());

        assertThat(LockWatchIntegrationTestUtilities.extractMetadata(update.events()))
                .containsExactly(
                        // From the first txn
                        Optional.of(LockRequestMetadata.of(
                                ImmutableMap.of(getDescriptor(TABLE_REF, CELL_1), CREATE_CHANGE_METADATA))),
                        // From the second txn
                        Optional.empty(),
                        // No lock event for the third txn since no locks were watched
                        // ...
                        // From the fourth txn
                        Optional.of(LockRequestMetadata.of(ImmutableMap.of(
                                getDescriptor(SERIALIZABLE_WATCHED_TABLE_REF, CELL_4), UPDATE_CHANGE_METADATA))));
    }

    @Test
    public void randomMetadataPassedAndFilteredCorrectly() {
        long randomSeed = System.currentTimeMillis();
        int numberOfIterations = 3;
        int numberOfRowsPerTransaction = 10;
        int numberOfCellsPerRow = 100;
        int maxWritesPerTransaction = 200;

        Random random = new Random(randomSeed);
        LockWatchVersion currentVersion = getCurrentVersion();
        ImmutableList.Builder<Optional<LockRequestMetadata>> expectedMetadataBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfIterations; i++) {
            Map<TableReference, Map<Cell, Optional<ChangeMetadata>>> values = generatedRandomCellsAndChangeMetadata(
                    numberOfRowsPerTransaction, numberOfCellsPerRow, maxWritesPerTransaction, random);
            expectedMetadataBuilder.add(getLockRequestMetadataForValues(values));
            putOrDeleteValuesMaybeWithMetadata(values, random);
        }

        TransactionsLockWatchUpdate update =
                getUpdateForTransactions(Optional.of(currentVersion), startSingleTransaction());
        assertThat(LockWatchIntegrationTestUtilities.extractMetadata(update.events()))
                .as("Expect to receive back the metadata we specified on previous transactions. Random seed: "
                        + randomSeed)
                .containsExactlyElementsOf(expectedMetadataBuilder.build());
    }

    private Runnable performWriteTransactionThatBlocksAfterLockingCells() {
        CountDownLatch endOfTest = new CountDownLatch(1);
        CountDownLatch inCommitBlock = new CountDownLatch(1);
        ExecutorService executor = PTExecutors.newSingleThreadExecutor();

        LockWatchIntegrationTestUtilities.CommitStageCondition<Void> blockingCondition =
                new LockWatchIntegrationTestUtilities.CommitStageCondition<>((_unused1, _unused2) -> {
                    inCommitBlock.countDown();
                    Uninterruptibles.awaitUninterruptibly(endOfTest);
                    return null;
                });

        executor.execute(() -> txnManager.runTaskWithConditionThrowOnConflict(blockingCondition, (txn, _unused) -> {
            blockingCondition.initialiseWithStartTimestamp(txn.getTimestamp());
            txn.put(TABLE_REF, ImmutableMap.of(CELL_1, DATA_1, CELL_4, DATA_4));
            return null;
        }));

        Uninterruptibles.awaitUninterruptibly(inCommitBlock);

        return () -> {
            endOfTest.countDown();
            executor.shutdown();
        };
    }

    private OpenTransaction startSingleTransaction() {
        return Iterables.getOnlyElement(txnManager.startTransactions(ImmutableList.of(PreCommitConditions.NO_OP)));
    }

    private TransactionsLockWatchUpdate getUpdateForTransactions(
            Optional<LockWatchVersion> currentVersion, OpenTransaction... transactions) {
        return LockWatchIntegrationTestUtilities.extractInternalLockWatchManager(txnManager)
                .getCache()
                .getEventCache()
                .getUpdateForTransactions(
                        Stream.of(transactions).map(Transaction::getTimestamp).collect(Collectors.toSet()),
                        currentVersion);
    }

    private LockWatchVersion getCurrentVersion() {
        return LockWatchIntegrationTestUtilities.extractInternalLockWatchManager(txnManager)
                .getCache()
                .getEventCache()
                .lastKnownVersion()
                .orElseThrow();
    }

    private void performWriteTransactionLockingAndUnlockingCells(Map<Cell, byte[]> values) {
        performWriteTransactionLockingAndUnlockingCells(TABLE_REF, values);
    }

    private void performWriteTransactionLockingAndUnlockingCells(TableReference tableRef, Map<Cell, byte[]> values) {
        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.put(tableRef, values);
            return null;
        });
    }

    private void performWriteMetadataTransactionLockingAndUnlockingCells(
            TableReference tableRef, Map<Cell, ValueAndChangeMetadata> valuesWithMetadata) {
        performTransactionTaskLockingAndUnlockingCells(txn -> {
            txn.putWithMetadata(tableRef, valuesWithMetadata);
            return null;
        });
    }

    private <T> void performTransactionTaskLockingAndUnlockingCells(TransactionTask<T, ?> task) {
        try {
            txnManager.runTaskThrowOnConflict(task);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LockWatchIntegrationTestUtilities.awaitAllUnlocked(txnManager);
    }

    private Set<LockDescriptor> lockedDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, LockEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> unlockedDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, UnlockEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> watchDescriptors(Collection<LockWatchEvent> events) {
        return getDescriptorsFromLockWatchEvent(events, WatchEventVisitor.INSTANCE);
    }

    private Set<LockDescriptor> getDescriptorsFromLockWatchEvent(
            Collection<LockWatchEvent> events, LockWatchEvent.Visitor<Set<LockDescriptor>> visitor) {
        return filterDescriptors(events.stream()
                .map(event -> event.accept(visitor))
                .flatMap(Set::stream)
                .collect(Collectors.toSet()));
    }

    private Set<LockDescriptor> getAllDescriptorsFromLockWatchEvent(
            Collection<LockWatchEvent> events, LockWatchEvent.Visitor<Set<LockDescriptor>> visitor) {
        return events.stream()
                .map(event -> event.accept(visitor))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> filterDescriptors(Set<LockDescriptor> descriptors) {
        return descriptors.stream()
                .filter(desc -> AtlasLockDescriptorUtils.tryParseTableRef(desc)
                        .orElseThrow()
                        .tableRef()
                        .equals(TABLE_REF))
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getDescriptors(Cell... cells) {
        return getDescriptors(TABLE_REF, cells);
    }

    private Set<LockDescriptor> getDescriptors(TableReference tableRef, Cell... cells) {
        return Stream.of(cells).map(cell -> getDescriptor(tableRef, cell)).collect(Collectors.toSet());
    }

    private Optional<LockRequestMetadata> lockAndGetEventMetadataFromLockWatchLog(LockRequest lockRequest) {
        LockWatchVersion currentVersion = getCurrentVersion();
        LockResponse response = txnManager.getTimelockService().lock(lockRequest);
        // We need to clean up the lock, otherwise it will be held forever (the TimeLock cluster is static)!
        txnManager.getTimelockService().unlock(ImmutableSet.of(response.getToken()));
        OpenTransaction newTxn = startSingleTransaction();
        List<LockWatchEvent> lockWatchEvents =
                getUpdateForTransactions(Optional.of(currentVersion), newTxn).events();
        newTxn.abort();
        List<Optional<LockRequestMetadata>> metadataList =
                LockWatchIntegrationTestUtilities.extractMetadata(lockWatchEvents);
        assertThat(metadataList).hasSize(1);
        return metadataList.get(0);
    }

    private LockRequest createTestLockRequest(
            Set<LockDescriptor> lockDescriptors, Optional<LockRequestMetadata> metadata) {
        return ImmutableLockRequest.builder()
                .lockDescriptors(lockDescriptors)
                .clientDescription("test-client")
                .acquireTimeoutMs(100)
                .metadata(metadata)
                .build();
    }

    private Set<LockDescriptor> extractDescriptorsFromUpdate(CommitUpdate commitUpdate) {
        return filterDescriptors(commitUpdate.accept(new CommitUpdate.Visitor<>() {
            @Override
            public Set<LockDescriptor> invalidateAll() {
                throw new SafeIllegalStateException("Should not be visiting invalidateAll update");
            }

            @Override
            public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return invalidatedLocks;
            }
        }));
    }

    private void putOrDeleteValuesMaybeWithMetadata(
            Map<TableReference, Map<Cell, Optional<ChangeMetadata>>> values, Random random) {
        performTransactionTaskLockingAndUnlockingCells(txn -> {
            values.forEach((tableRef, tableValues) ->
                    tableValues.forEach((cell, optChangeMetadata) -> optChangeMetadata.ifPresentOrElse(
                            changeMetadata -> {
                                if (random.nextBoolean()) {
                                    txn.putWithMetadata(
                                            tableRef,
                                            ImmutableMap.of(
                                                    cell,
                                                    ValueAndChangeMetadata.of(
                                                            PtBytes.toBytes(random.nextLong()), changeMetadata)));
                                } else {
                                    txn.deleteWithMetadata(tableRef, ImmutableMap.of(cell, changeMetadata));
                                }
                            },
                            () -> {
                                if (random.nextBoolean()) {
                                    txn.put(tableRef, ImmutableMap.of(cell, PtBytes.toBytes(random.nextLong())));
                                } else {
                                    txn.delete(tableRef, ImmutableSet.of(cell));
                                }
                            })));
            return null;
        });
    }

    private static boolean isWatchedTable(TableReference tableRef) {
        return ImmutableSet.of(TABLE_REF, SERIALIZABLE_WATCHED_TABLE_REF).contains(tableRef);
    }

    private static Map<TableReference, Map<Cell, Optional<ChangeMetadata>>> generatedRandomCellsAndChangeMetadata(
            int numberOfRowsPerTransaction, int numberOfCellsPerRow, int maxWritesPerTransaction, Random random) {
        List<Cell> availableCells = IntStream.range(0, numberOfRowsPerTransaction)
                .boxed()
                .flatMap(rowIndex -> IntStream.range(0, numberOfCellsPerRow)
                        .boxed()
                        .map(cellIndex ->
                                Cell.create(PtBytes.toBytes("row" + rowIndex), PtBytes.toBytes("col" + cellIndex))))
                .collect(Collectors.toList());
        Set<Cell> chosenCells = IntStream.range(0, maxWritesPerTransaction)
                .mapToObj(_unused -> availableCells.get(random.nextInt(availableCells.size())))
                .collect(Collectors.toUnmodifiableSet());

        // 50/25/25 split of cells between the watched, unwatched, and serializable watched table
        Set<Cell> tableOneCells =
                chosenCells.stream().filter(_cell -> random.nextBoolean()).collect(Collectors.toUnmodifiableSet());
        Set<Cell> tableTwoCells = chosenCells.stream()
                .filter(cell -> random.nextBoolean() && !tableOneCells.contains(cell))
                .collect(Collectors.toUnmodifiableSet());
        Set<Bytes> seenRowsForMetadata = new HashSet<>();
        Map<Cell, Optional<ChangeMetadata>> serializableTableCellsUniqueRows = KeyedStream.ofEntries(
                        maybeAssociateMetadata(
                                chosenCells.stream()
                                        .filter(cell -> !tableOneCells.contains(cell) && !tableTwoCells.contains(cell))
                                        .collect(Collectors.toUnmodifiableSet()),
                                random)
                                .entrySet()
                                .stream())
                // Ensure unique rows if metadata is present to prevent commit from failing due to SERIALIZABLE conflict
                // handler
                .filterEntries((cell, optMetadata) ->
                        optMetadata.isEmpty() || seenRowsForMetadata.add(Bytes.from(cell.getRowName())))
                .collectToMap();
        // For each cell, 50/50 split on whether it should have metadata
        return ImmutableMap.of(
                TABLE_REF,
                maybeAssociateMetadata(tableOneCells, random),
                TABLE_2_REF,
                maybeAssociateMetadata(tableTwoCells, random),
                SERIALIZABLE_WATCHED_TABLE_REF,
                serializableTableCellsUniqueRows);
    }

    private static Map<Cell, Optional<ChangeMetadata>> maybeAssociateMetadata(Set<Cell> cells, Random random) {
        return KeyedStream.of(cells.stream())
                .<Optional<ChangeMetadata>>map(_cell ->
                        random.nextBoolean() ? Optional.of(generateRandomChangeMetadata(random)) : Optional.empty())
                .collectToMap();
    }

    private static ChangeMetadata generateRandomChangeMetadata(Random random) {
        int index = random.nextInt(4);
        switch (index) {
            case 0:
                return ChangeMetadata.created(PtBytes.toBytes(random.nextLong()));
            case 1:
                return ChangeMetadata.unchanged();
            case 2:
                return ChangeMetadata.updated(PtBytes.toBytes(random.nextLong()), PtBytes.toBytes(random.nextLong()));
            case 3:
                return ChangeMetadata.deleted(PtBytes.toBytes(random.nextLong()));
        }
        throw new RuntimeException("unreachable");
    }

    private static Optional<LockRequestMetadata> getLockRequestMetadataForValues(
            Map<TableReference, Map<Cell, Optional<ChangeMetadata>>> values) {
        Map<LockDescriptor, ChangeMetadata> locks = KeyedStream.ofEntries(values.entrySet().stream())
                .filterKeys(LockWatchEventIntegrationTest::isWatchedTable)
                .flatMapEntries((table, cellToMetadata) -> KeyedStream.ofEntries(cellToMetadata.entrySet().stream())
                        .flatMap(Optional::stream)
                        .mapKeys(cell -> getDescriptor(table, cell))
                        .entries())
                .collectToMap();
        return locks.isEmpty() ? Optional.empty() : Optional.of(LockRequestMetadata.of(locks));
    }

    private static LockDescriptor getDescriptor(TableReference tableRef, Cell cell) {
        // SERIALIZABLE conflict handler uses row-level locks only
        if (tableRef.equals(SERIALIZABLE_WATCHED_TABLE_REF)) {
            return AtlasRowLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName());
        }
        return AtlasCellLockDescriptor.of(tableRef.getQualifiedName(), cell.getRowName(), cell.getColumnName());
    }

    private static final class LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final LockEventVisitor INSTANCE = new LockEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return lockEvent.lockDescriptors();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return ImmutableSet.of();
        }
    }

    private static final class UnlockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final UnlockEventVisitor INSTANCE = new UnlockEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return unlockEvent.lockDescriptors();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return ImmutableSet.of();
        }
    }

    private static final class WatchEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        static final WatchEventVisitor INSTANCE = new WatchEventVisitor();

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return lockWatchCreatedEvent.lockDescriptors();
        }
    }

    private final class CommitUpdateExtractingCondition
            extends LockWatchIntegrationTestUtilities.CommitStageCondition<CommitUpdate> {
        public CommitUpdateExtractingCondition() {
            super((startTs, _unused) -> {
                LockWatchManagerInternal lockWatchManager =
                        LockWatchIntegrationTestUtilities.extractInternalLockWatchManager(txnManager);
                return lockWatchManager.getCache().getEventCache().getCommitUpdate(startTs);
            });
        }
    }
}
