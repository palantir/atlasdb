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
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.cache.HitDigest;
import com.palantir.atlasdb.keyvalue.api.cache.LockWatchValueScopingCache;
import com.palantir.atlasdb.keyvalue.api.cache.TransactionScopedCache;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.OpenTransaction;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class SimpleEteLockWatchResource implements EteLockWatchResource {
    public static final Namespace NAMESPACE = Namespace.create("lock");
    public static final byte[] COLUMN = PtBytes.toBytes("b");
    private static final TableMetadata TABLE_METADATA = TableMetadata.builder()
            .conflictHandler(ConflictHandler.RETRY_ON_WRITE_WRITE_CELL)
            .nameLogSafety(LogSafety.SAFE)
            .build();

    private final TransactionManager transactionManager;
    private final LockWatchManagerInternal lockWatchManager;
    private final Map<TransactionId, TransactionAndCondition> activeTransactions = new ConcurrentHashMap<>();

    private String table = "watch";
    private TableReference lockWatchTable = TableReference.create(NAMESPACE, table);

    public SimpleEteLockWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        createTable();
        this.lockWatchManager = (LockWatchManagerInternal) transactionManager.getLockWatchManager();
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
        return Optional.ofNullable(txnAndCondition.condition().getCommitUpdate());
    }

    @Override
    public HitDigest endTransactionWithDigest(TransactionId transactionId) {
        Transaction transaction = activeTransactions.get(transactionId).transaction();
        TransactionScopedCache transactionScopedCache = ((LockWatchValueScopingCache)
                        lockWatchManager.getCache().getValueCache())
                .createTransactionScopedCache(transaction.getTimestamp());
        HitDigest hitDigest = transactionScopedCache.getHitDigest();
        endTransaction(transactionId);
        return hitDigest;
    }

    @Override
    public void write(WriteRequest writeRequest) {
        Transaction transaction = activeTransactions.get(writeRequest.id()).transaction();
        Map<Cell, byte[]> byteValues = getValueMap(writeRequest.rows());
        transaction.put(lockWatchTable, byteValues);
    }

    @Override
    public ReadResponse read(ReadRequest readRequest) {
        Transaction transaction = activeTransactions.get(readRequest.id()).transaction();
        return ImmutableReadResponse.of(
                KeyedStream.stream(transaction.get(lockWatchTable, getCells(readRequest.rows())))
                        .map(ImmutableResult::of)
                        .values()
                        .collect(Collectors.toSet()));
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
        keyValueService.createTable(lockWatchTable, TABLE_METADATA.persistToBytes());
        transactionManager
                .getLockWatchManager()
                .registerPreciselyWatches(
                        ImmutableSet.of(LockWatchReferences.entireTable(lockWatchTable.getQualifiedName())));
    }

    private Set<Cell> getCells(Set<String> rows) {
        return rows.stream()
                .map(PtBytes::toBytes)
                .map(row -> Cell.create(row, COLUMN))
                .collect(Collectors.toSet());
    }

    private Map<Cell, byte[]> getValueMap(Map<String, String> rows) {
        return KeyedStream.stream(rows)
                .mapKeys(PtBytes::toBytes)
                .mapKeys(row -> Cell.create(row, COLUMN))
                .map(PtBytes::toBytes)
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

    final class CommitUpdateCondition implements PreCommitCondition {
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
