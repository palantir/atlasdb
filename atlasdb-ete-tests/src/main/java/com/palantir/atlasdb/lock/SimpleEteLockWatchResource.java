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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleEteLockWatchResource implements EteLockWatchResource {
    public static final String TABLE = "watch";

    private static final Logger log = LoggerFactory.getLogger(SimpleEteLockWatchResource.class);
    public static final TableReference LOCK_WATCH_TABLE = TableReference.create(Namespace.create("lock"), TABLE);
    private static final byte[] VALUE = PtBytes.toBytes("value");
    private static final byte[] COLUMN = PtBytes.toBytes("b");

    private final TransactionManager transactionManager;
    private final ExposedLockWatchManager lockWatchManager;
    private final Map<TransactionId, TransactionAndCondition> activeTransactions = new ConcurrentHashMap<>();

    public SimpleEteLockWatchResource(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        KeyValueService keyValueService = transactionManager.getKeyValueService();
        keyValueService.createTable(LOCK_WATCH_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        transactionManager
                .getLockWatchManager()
                .registerWatches(ImmutableSet.of(LockWatchReferences.entireTable(LOCK_WATCH_TABLE.getQualifiedName())));
        lockWatchManager = new ExposedLockWatchManager(transactionManager.getLockWatchManager());
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
    public CommitUpdate endTransaction(TransactionId transactionId) {
        log.info("I am ending transaction");
        TransactionAndCondition txnAndCondition = activeTransactions.remove(transactionId);
        commit(txnAndCondition);
        txnAndCondition.condition().cleanup();
        log.info("Transaction completed!");
        return txnAndCondition.condition().getCommitUpdate();
    }

    @Override
    public void write(WriteRequest writeRequest) {
        log.info("I have now started the write");
        Transaction transaction = activeTransactions.get(writeRequest.id()).transaction();
        Map<Cell, byte[]> byteValues = getValueMap(writeRequest.rows());
        transaction.put(LOCK_WATCH_TABLE, byteValues);
        log.info("I have now finished the write");
    }

    @Override
    public TransactionsLockWatchUpdate getUpdate(GetLockWatchUpdateRequest updateRequest) {
        return lockWatchManager.getUpdateForTransactions(updateRequest.startTimestamps(), updateRequest.version());
    }

    @Override
    public LockWatchVersion getVersion(TransactionId transactionId) {
        return lockWatchManager
                .getUpdateForTransactions(ImmutableSet.of(transactionId.startTs()), Optional.empty())
                .startTsToSequence()
                .get(transactionId.startTs());
    }

    private Map<Cell, byte[]> getValueMap(Set<String> rows) {
        return KeyedStream.of(rows)
                .mapKeys(PtBytes::toBytes)
                .mapKeys(row -> Cell.create(row, COLUMN))
                .map(unused -> VALUE)
                .collectToMap();
    }

    private void commit(TransactionAndCondition txnAndCondition) {
        txnAndCondition.transaction().commit();
        txnAndCondition.transaction().finish(unused -> null);
    }

    private CommitUpdate getCommitUpdateInternal(long startTs) {
        return lockWatchManager.getCommitUpdate(startTs);
    }

    @Value.Immutable
    interface TransactionAndCondition {

        static TransactionAndCondition of(OpenTransaction transaction, CommitUpdateCondition condition) {
            return ImmutableTransactionAndCondition.of(transaction, condition);
        }

        @Value.Parameter
        OpenTransaction transaction();

        @Value.Parameter
        CommitUpdateCondition condition();
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
            if (startTs.map(ts -> ts != timestamp).orElse(false)) {
                commitUpdate.set(getCommitUpdateInternal(startTs.get()));
            }
        }
    }
}
