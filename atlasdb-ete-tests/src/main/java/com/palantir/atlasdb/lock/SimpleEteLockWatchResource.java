/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.lock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.watch.ExposedLockWatchManager;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.immutables.value.Value;

public final class SimpleEteLockWatchResource implements EteLockWatchResource {
    public static final Namespace NAMESPACE = Namespace.create("lock");
    private static final byte[] VALUE = PtBytes.toBytes("value");
    private static final byte[] COLUMN = PtBytes.toBytes("b");

    private final TransactionManager transactionManager;
    private final ExposedLockWatchManager lockWatchManager;
    private final Map<TransactionId, TransactionAndCondition> activeTransactions = new ConcurrentHashMap<>();

    private String table = "watch";
    private TableReference lockWatchTable = TableReference.create(NAMESPACE, table);

    public SimpleEteLockWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        createTable();
        this.lockWatchManager = new ExposedLockWatchManager(transactionManager.getLockWatchManager());
    }

    @Override
    public TransactionId startTransaction() {
        CommitUpdateCondition condition = new CommitUpdateCondition();
        OpenTransaction transaction =
                Iterables.getOnlyElement(transactionManager.startTransactions(ImmutableList.of(condition)));
        TransactionId id = TransactionId.of(transaction.getTimestamp());
        condition.setStartTs(transaction.getTimestamp());
        activeTransactions.put(id, TransactionAndCondition.of(transaction, condition));
        return id;
    }

    @Override
    public Optional<CommitUpdate> endTransaction(TransactionId transactionId) {
        TransactionAndCondition txnAndCondition = activeTransactions.remove(transactionId);
        txnAndCondition.transaction().commit();
        txnAndCondition.transaction().finish(unused -> null);
        txnAndCondition.condition().cleanup();

        // Wait until the unlock request, which is async, is processed
        Awaitility.await("unlock requests processed")
                .atMost(Duration.ofSeconds(5))
                .until(() -> {
                    // Empty transaction will still get an update for lock watches
                    transactionManager.runTaskThrowOnConflict(txn -> null);
                    return lockWatchManager
                            .getCache()
                            .getEventCache()
                            .lastKnownVersion()
                            .map(LockWatchVersion::version)
                            .filter(version -> version % 2 == 0)
                            .isPresent();
                });
        return Optional.ofNullable(txnAndCondition.condition().getCommitUpdate());
    }

    @Override
    public void write(WriteRequest writeRequest) {
        Transaction transaction = activeTransactions.get(writeRequest.id()).transaction();
        Map<Cell, byte[]> byteValues = getValueMap(writeRequest.rows());
        transaction.put(lockWatchTable, byteValues);
    }

    @Override
    public TransactionsLockWatchUpdate getUpdate(GetLockWatchUpdateRequest updateRequest) {
        return lockWatchManager.getUpdateForTransactions(updateRequest.startTimestamps(), updateRequest.version());
    }

    @Override
    public void setTable(String tableName) {
        table = tableName;
        lockWatchTable = TableReference.create(NAMESPACE, table);
        createTable();
    }

    private void createTable() {
        KeyValueService keyValueService = transactionManager.getKeyValueService();
        keyValueService.createTable(lockWatchTable, AtlasDbConstants.GENERIC_TABLE_METADATA);
        transactionManager
                .getLockWatchManager()
                .registerPreciselyWatches(
                        ImmutableSet.of(LockWatchReferences.entireTable(lockWatchTable.getQualifiedName())));
    }

    private Map<Cell, byte[]> getValueMap(Set<String> rows) {
        return KeyedStream.of(rows)
                .mapKeys(PtBytes::toBytes)
                .mapKeys(row -> Cell.create(row, COLUMN))
                .map(unused -> VALUE)
                .collectToMap();
    }

    @Value.Immutable
    interface TransactionAndCondition {
        @Value.Parameter
        OpenTransaction transaction();

        @Value.Parameter
        CommitUpdateCondition condition();

        static TransactionAndCondition of(OpenTransaction transaction, CommitUpdateCondition condition) {
            return ImmutableTransactionAndCondition.of(transaction, condition);
        }
    }

    class CommitUpdateCondition implements PreCommitCondition {
        private final AtomicReference<CommitUpdate> commitUpdate = new AtomicReference<>();
        private Optional<Long> startTs = Optional.empty();

        CommitUpdateCondition() {}

        void setStartTs(long startTs) {
            this.startTs = Optional.of(startTs);
        }

        CommitUpdate getCommitUpdate() {
            return commitUpdate.get();
        }

        @Override
        public void throwIfConditionInvalid(long timestamp) {
            startTs.filter(ts -> ts != timestamp)
                    .ifPresent(ts -> commitUpdate.set(lockWatchManager.getCommitUpdate(ts)));
        }
    }
}
