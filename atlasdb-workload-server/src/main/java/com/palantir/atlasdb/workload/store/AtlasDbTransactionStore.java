/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.PreCommitCondition;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import one.util.streamex.EntryStream;

public final class AtlasDbTransactionStore implements InteractiveTransactionStore {

    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbTransactionStore.class);
    private final TransactionManager transactionManager;

    private final Map<String, TableReference> tables;

    private AtlasDbTransactionStore(TransactionManager transactionManager, Map<String, TableReference> tables) {
        this.transactionManager = transactionManager;
        this.tables = tables;
    }

    @Override
    public Optional<Integer> get(String table, WorkloadCell cell) {
        return transactionManager.runTaskWithRetry(
                transaction -> new AtlasDbInteractiveTransaction(transaction, tables).read(table, cell));
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions) {
        return readWrite(txn -> {
            AtlasDbTransactionActionVisitor visitor = new AtlasDbTransactionActionVisitor(txn);
            actions.forEach(action -> action.accept(visitor));
        });
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(Consumer<InteractiveTransaction> interactiveTransactionConsumer) {
        AtomicReference<List<WitnessedTransactionAction>> witnessedActionsReference = new AtomicReference<>();
        Supplier<CommitTimestampProvider> commitTimestampProvider =
                Suppliers.memoize(() -> new CommitTimestampProvider());
        try {
            Transaction transaction =
                    transactionManager.runTaskWithConditionWithRetry(commitTimestampProvider, (txn, _condition) -> {
                        AtlasDbInteractiveTransaction atlasDbInteractiveTransaction =
                                new AtlasDbInteractiveTransaction(txn, tables);
                        interactiveTransactionConsumer.accept(atlasDbInteractiveTransaction);
                        witnessedActionsReference.set(atlasDbInteractiveTransaction.witness());
                        return txn;
                    });

            if (transaction.isAborted()) {
                return Optional.empty();
            }

            return Optional.of(ImmutableWitnessedTransaction.builder()
                    .startTimestamp(transaction.getTimestamp())
                    .commitTimestamp(commitTimestampProvider
                            .get()
                            .getCommitTimestampOrThrowIfMaybeNotCommitted(transaction.getTimestamp()))
                    .actions(witnessedActionsReference.get())
                    .build());
        } catch (SafeIllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            // TODO: Need to eventually handle PuE exceptions, as they could've succeeded in committing.
            log.info("Failed to record transaction due to an exception", e);
            return Optional.empty();
        }
    }

    private static class AtlasDbTransactionActionVisitor implements TransactionActionVisitor<Void> {

        private final InteractiveTransaction transaction;

        public AtlasDbTransactionActionVisitor(InteractiveTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public Void visit(ReadTransactionAction readTransactionAction) {
            transaction.read(readTransactionAction.table(), readTransactionAction.cell());
            return null;
        }

        @Override
        public Void visit(WriteTransactionAction writeTransactionAction) {
            transaction.write(
                    writeTransactionAction.table(), writeTransactionAction.cell(), writeTransactionAction.value());
            return null;
        }

        @Override
        public Void visit(DeleteTransactionAction deleteTransactionAction) {
            transaction.delete(deleteTransactionAction.table(), deleteTransactionAction.cell());
            return null;
        }
    }

    public static AtlasDbTransactionStore create(
            TransactionManager transactionManager, Map<TableReference, byte[]> tables) {
        transactionManager.getKeyValueService().createTables(tables);
        Map<String, TableReference> tableMapping = EntryStream.of(tables)
                .keys()
                .mapToEntry(TableReference::getTableName, Function.identity())
                .toMap();
        return new AtlasDbTransactionStore(transactionManager, tableMapping);
    }

    @VisibleForTesting
    @NotThreadSafe
    static final class CommitTimestampProvider implements PreCommitCondition {

        private Optional<Long> maybeCommitOrStartTimestamp = Optional.empty();

        @Override
        public void throwIfConditionInvalid(long timestamp) {
            maybeCommitOrStartTimestamp = Optional.of(timestamp);
        }

        public Optional<Long> getCommitTimestampOrThrowIfMaybeNotCommitted(long startTimestamp) {
            long commitOrStartTimestamp = maybeCommitOrStartTimestamp.orElseThrow(() -> new SafeIllegalStateException(
                    "Timestamp has not been set, which means the pre commit condition has never been executed,"
                            + " thus the transaction never committed."));
            if (startTimestamp == commitOrStartTimestamp) {
                return Optional.empty();
            }

            return Optional.of(commitOrStartTimestamp);
        }
    }
}
