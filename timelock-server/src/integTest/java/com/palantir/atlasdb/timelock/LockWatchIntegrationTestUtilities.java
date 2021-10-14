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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MoreCollectors;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.watch.*;
import com.palantir.logsafe.Preconditions;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.awaitility.Awaitility;

public final class LockWatchIntegrationTestUtilities {
    private static final String TEST_PACKAGE = "package";
    private static final String TABLE = "table";

    private LockWatchIntegrationTestUtilities() {
        // no-op
    }

    /**
     * By requesting a snapshot, we get a single snapshot event that contains all currently held lock descriptors.
     * As unlocks are asynchronous, we must wait for the unlocks to go through to guarantee consistent testing.
     */
    public static void awaitAllUnlocked(TransactionManager txnManager) {
        LockWatchManagerInternal lockWatchManager = extractInternalLockWatchManager(txnManager);
        Awaitility.await("All descriptors are unlocked")
                .atMost(Duration.ofSeconds(5))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> getLockWatchState(txnManager, lockWatchManager)
                        .map(event -> event.accept(new LockWatchEvent.Visitor<Boolean>() {
                            @Override
                            public Boolean visit(LockEvent lockEvent) {
                                return false;
                            }

                            @Override
                            public Boolean visit(UnlockEvent unlockEvent) {
                                return false;
                            }

                            @Override
                            public Boolean visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
                                return lockWatchCreatedEvent.lockDescriptors().isEmpty();
                            }
                        }))
                        .orElse(false));
    }

    /**
     * The lock watch manager registers watch events every five seconds - therefore, tables may not be watched
     * immediately after a TimeLock leader election. By requesting an update with an empty version, we guarantee that
     * we only receive a single event containing a snapshot of the current lock watch event state, including watched
     * tables.
     */
    public static void awaitTableWatched(TransactionManager txnManager, TableReference tableReference) {
        LockWatchManagerInternal lockWatchManager = extractInternalLockWatchManager(txnManager);
        Awaitility.await("Tables are watched")
                .atMost(Duration.ofSeconds(5))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> getLockWatchState(txnManager, lockWatchManager)
                        .map(event -> event.accept(new LockWatchEvent.Visitor<Boolean>() {
                            @Override
                            public Boolean visit(LockEvent lockEvent) {
                                return false;
                            }

                            @Override
                            public Boolean visit(UnlockEvent unlockEvent) {
                                return false;
                            }

                            @Override
                            public Boolean visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
                                return lockWatchCreatedEvent
                                        .references()
                                        .contains(LockWatchReferences.entireTable(tableReference.getQualifiedName()));
                            }
                        }))
                        .orElse(false));
    }

    /**
     * The internal version of the lock watch manager is hidden from the user, both to reduce API surface area, and
     * because certain classes aren't visible everywhere.
     */
    public static LockWatchManagerInternal extractInternalLockWatchManager(TransactionManager txnManager) {
        return (LockWatchManagerInternal) txnManager.getLockWatchManager();
    }

    public static TransactionManager createTransactionManager(
            double validationProbability, TestableTimelockCluster timelockCluster, String namespace) {
        return TimeLockTestUtils.createTransactionManager(
                        timelockCluster,
                        namespace,
                        AtlasDbRuntimeConfig.defaultRuntimeConfig(),
                        ImmutableAtlasDbConfig.builder()
                                .lockWatchCaching(LockWatchCachingConfig.builder()
                                        .validationProbability(validationProbability)
                                        .build()),
                        Optional.empty(),
                        createSchema())
                .transactionManager();
    }

    private static Optional<LockWatchEvent> getLockWatchState(
            TransactionManager txnManager, LockWatchManagerInternal lockWatchManager) {
        return txnManager.runTaskThrowOnConflict(txn -> lockWatchManager
                .getCache()
                .getEventCache()
                .getUpdateForTransactions(ImmutableSet.of(txn.getTimestamp()), Optional.empty())
                .events()
                .stream()
                .collect(MoreCollectors.toOptional()));
    }

    private static Schema createSchema() {
        Schema schema = new Schema("table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
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

    public static final class CommitStageCondition<T> implements PreCommitCondition {
        private final Function<Long, T> startTimestampFunction;
        private final AtomicReference<T> commitStageResult;
        private volatile Optional<Long> startTimestamp;

        public CommitStageCondition(Function<Long, T> startTimestampFunction, AtomicReference<T> commitStageResult) {
            this.startTimestampFunction = startTimestampFunction;
            this.commitStageResult = commitStageResult;
            this.startTimestamp = Optional.empty();
        }

        public void setStartTimestamp(long startTs) {
            this.startTimestamp = Optional.of(startTs);
        }

        public T getCommitStageResult() {
            return commitStageResult.get();
        }

        @Override
        public void throwIfConditionInvalid(long timestamp) {
            Preconditions.checkState(startTimestamp.isPresent(), "Must initialise start timestamp immediately");
            long startTs = startTimestamp.get();

            if (startTs != timestamp) {
                this.commitStageResult.set(startTimestampFunction.apply(startTs));
            }
        }
    }
}
