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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.lock.TransactionId;
import com.palantir.atlasdb.lock.WriteRequest;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.flake.ShouldRetry;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;
import static org.assertj.core.api.Assertions.assertThat;

public final class LockWatchEventIntegrationTest {
    private static final String TEST_PACKAGE = "package";
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
            generateThreeNodeTimelockCluster(9096, builder -> builder.clientPaxosBuilder(
                            builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                    .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)));

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private static final byte[] ROW_1 = PtBytes.toBytes("final");
    private static final byte[] ROW_2 = PtBytes.toBytes("destination");
    private static final byte[] ROW_3 = PtBytes.toBytes("awaits");
    private static final ImmutableList<byte[]> ROWS = ImmutableList.of(ROW_1, ROW_2, ROW_3);
    private static final byte[] COL_1 = PtBytes.toBytes("parthenon");
    private static final byte[] COL_2 = PtBytes.toBytes("had");
    private static final byte[] COL_3 = PtBytes.toBytes("columns");
    private static final ImmutableList<byte[]> COLS = ImmutableList.of(COL_1, COL_2, COL_3);
    private static final String SEED = "seed";

    private TransactionManager txnManager;

    @Before
    public void before() {
        createTransactionManager(0.0);
        awaitTableWatched();
    }

    final class CommitUpdateExtractingCondition implements PreCommitCondition {
        private final AtomicLong startTimestamp = new AtomicLong(-1L);
        private CommitUpdate commitUpdate;

        @Override
        public void throwIfConditionInvalid(long timestamp) {}
    }

    @Test
    public void commitUpdatesDoNotContainTheirOwnCommitLocks() {

        TransactionId firstTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(firstTxn, ROW_1));

        TransactionId secondTxn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(secondTxn, ROW_2));

        CommitUpdate firstUpdate = lockWatcher.endTransaction(firstTxn).get();
        CommitUpdate secondUpdate = lockWatcher.endTransaction(secondTxn).get();

        assertThat(extractDescriptorsFromUpdate(firstUpdate)).isEmpty();
        assertThat(extractDescriptorsFromUpdate(secondUpdate))
                .containsExactlyInAnyOrderElementsOf(getDescriptors(ROW_1));
    }

    @Test
    @ShouldRetry
    public void multipleTransactionVersionsReturnsSnapshotAndOnlyRelevantRecentEvents() {
        LockWatchVersion baseVersion = seedCacheAndGetVersion();

        writeValues(ROW_1, ROW_2);
        TransactionId secondTxn = lockWatcher.startTransaction();

        writeValues(row(3));
        TransactionId fourthTxn = lockWatcher.startTransaction();

        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(GetLockWatchUpdateRequest.of(
                ImmutableSet.of(secondTxn.startTs(), fourthTxn.startTs()), Optional.empty()));

        assertThat(update.clearCache()).isTrue();
        assertThat(update.startTsToSequence().get(secondTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(fourthTxn.startTs()).version())
                .isEqualTo(baseVersion.version() + 4);
        assertThat(lockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(watchDescriptors(update.events())).isEmpty();

        lockWatcher.endTransaction(secondTxn);
        lockWatcher.endTransaction(fourthTxn);
    }

    @Test
    @ShouldRetry // TODO(jshah): Unlocks are async, and thus not always registered
    public void upToDateVersionReturnsOnlyNecessaryEvents() {
        LockWatchVersion baseVersion = seedCacheAndGetVersion();

        writeValues(ROW_1);
        TransactionId firstTxn = lockWatcher.startTransaction();
        writeValues(ROW_2);
        LockWatchVersion currentVersion = getCurrentVersion();
        writeValues(ROW_3);
        TransactionId secondTxn = lockWatcher.startTransaction();

        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(GetLockWatchUpdateRequest.of(
                ImmutableSet.of(firstTxn.startTs(), secondTxn.startTs()), Optional.of(currentVersion)));

        assertThat(update.clearCache()).isFalse();
        assertThat(update.startTsToSequence().get(firstTxn.startTs()).version()).isEqualTo(baseVersion.version() + 2);
        assertThat(update.startTsToSequence().get(secondTxn.startTs()).version())
                .isEqualTo(currentVersion.version() + 2);
        assertThat(lockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(unlockedDescriptors(update.events())).containsExactlyInAnyOrderElementsOf(getDescriptors(row(3)));
        assertThat(watchDescriptors(update.events())).isEmpty();
    }

    private void writeValues(String... rows) {
        TransactionId txn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(txn, rows));
        lockWatcher.endTransaction(txn);
    }

    private LockWatchVersion seedCacheAndGetVersion() {
        TransactionId txn = lockWatcher.startTransaction();
        lockWatcher.write(WriteRequest.of(txn, SEED));
        lockWatcher.endTransaction(txn);

        return getCurrentVersion();
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

    private LockWatchVersion getCurrentVersion() {
        TransactionId emptyTxn = lockWatcher.startTransaction();
        TransactionsLockWatchUpdate update = lockWatcher.getUpdate(
                GetLockWatchUpdateRequest.of(ImmutableSet.of(emptyTxn.startTs()), Optional.empty()));
        LockWatchVersion version = update.startTsToSequence().get(emptyTxn.startTs());
        lockWatcher.endTransaction(emptyTxn);
        return version;
    }

    private Set<LockDescriptor> filterDescriptors(Set<LockDescriptor> descriptors) {
        return descriptors.stream()
                .filter(desc -> AtlasLockDescriptorUtils.tryParseTableRef(desc)
                        .get()
                        .tableRef()
                        .equals(tableReference))
                .collect(Collectors.toSet());
    }

    private Set<LockDescriptor> getDescriptors(String... rows) {
        return Stream.of(rows)
                .map(row -> AtlasRowLockDescriptor.of(this.tableReference.getQualifiedName(), PtBytes.toBytes(row)))
                .collect(Collectors.toSet());
    }

    private static String row(int index) {
        return "row" + index;
    }

    private Set<LockDescriptor> extractDescriptorsFromUpdate(CommitUpdate commitUpdate) {
        return filterDescriptors(commitUpdate.accept(new CommitUpdate.Visitor<Set<LockDescriptor>>() {
            @Override
            public Set<LockDescriptor> invalidateAll() {
                return ImmutableSet.of();
            }

            @Override
            public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return invalidatedLocks;
            }
        }));
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

    private LockWatchManagerInternal extractInternalLockWatchManager() {
        return (LockWatchManagerInternal) txnManager.getLockWatchManager();
    }

    private LockWatchValueScopingCache extractValueCache() {
        return (LockWatchValueScopingCache)
                extractInternalLockWatchManager().getCache().getValueCache();
    }

    private TransactionScopedCache extractTransactionCache(Transaction txn) {
        return extractValueCache().getTransactionScopedCache(txn.getTimestamp());
    }

    /**
     * Lock watch events tend to come in pairs - a lock and an unlock event. However, unlocks are asynchronous, and
     * thus we need to wait until we have received the unlock event before proceeding for deterministic testing
     * behaviour.
     */
    private void awaitUnlock() {
        awaitLockWatches(version -> version % 2 == 0);
    }

    /**
     * The lock watch manager registers watch events every five seconds - therefore, tables may not be watched
     * immediately after a Timelock leader election.
     */
    private void awaitTableWatched() {
        awaitLockWatches(version -> version > -1);
    }

    private void awaitLockWatches(Predicate<Long> versionPredicate) {
        LockWatchManagerInternal lockWatchManager = extractInternalLockWatchManager();
        Awaitility.await()
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
                            .filter(versionPredicate)
                            .isPresent();
                });
    }

    private void createTransactionManager(double validationProbability) {
        txnManager = TimeLockTestUtils.createTransactionManager(
                        CLUSTER,
                        Namespace.DEFAULT_NAMESPACE.getName(),
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
