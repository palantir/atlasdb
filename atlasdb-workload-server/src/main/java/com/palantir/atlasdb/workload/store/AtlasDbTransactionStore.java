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
import com.palantir.atlasdb.transaction.api.TransactionCommitFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.InteractiveTransaction;
import com.palantir.atlasdb.workload.transaction.RowColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.SingleCellReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.FullyWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.MaybeWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.logsafe.SafeArg;
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
    private final AtlasDbTransactionConcluder transactionConcluder;

    private final Map<String, TableReference> tables;
    private final Runnable preTransactionTask;

    private AtlasDbTransactionStore(
            TransactionManager transactionManager, Map<String, TableReference> tables, Runnable preTransactionTask) {
        this.transactionManager = transactionManager;
        this.transactionConcluder = new AtlasDbTransactionConcluder(transactionManager.getTransactionService());
        this.tables = tables;
        this.preTransactionTask = preTransactionTask;
    }

    @Override
    public Optional<Integer> get(String table, WorkloadCell cell) {
        preTransactionTask.run();
        return transactionManager.runTaskWithRetry(
                transaction -> new AtlasDbInteractiveTransaction(transaction, tables).read(table, cell));
    }

    @Override
    public boolean isCommittedForcingTransactionConclusion(long startTimestamp) {
        TransactionStatus status = transactionConcluder.forceTransactionConclusion(startTimestamp);
        return status instanceof TransactionStatus.Committed;
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions) {
        preTransactionTask.run();
        return readWrite(txn -> {
            AtlasDbTransactionActionVisitor visitor = new AtlasDbTransactionActionVisitor(txn);
            actions.forEach(action -> action.accept(visitor));
        });
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(Consumer<InteractiveTransaction> interactiveTransactionConsumer) {
        preTransactionTask.run();

        AtomicReference<List<WitnessedTransactionAction>> witnessedActionsReference = new AtomicReference<>();
        AtomicReference<Transaction> transactionReference = new AtomicReference<>();
        Supplier<CommitTimestampProvider> commitTimestampProvider =
                Suppliers.memoize(() -> new CommitTimestampProvider());
        try {
            transactionManager.runTaskWithConditionWithRetry(commitTimestampProvider, (txn, _condition) -> {
                log.info("Starting transaction with timestamp {}", SafeArg.of("startTimestamp", txn.getTimestamp()));
                transactionReference.set(txn);
                AtlasDbInteractiveTransaction atlasDbInteractiveTransaction =
                        new AtlasDbInteractiveTransaction(txn, tables);
                interactiveTransactionConsumer.accept(atlasDbInteractiveTransaction);
                witnessedActionsReference.set(atlasDbInteractiveTransaction.witness());
                log.info(
                        "Should have finished transaction body with timestamp {}",
                        SafeArg.of("startTimestamp", txn.getTimestamp()));
                return null;
            });

            Transaction transaction = transactionReference.get();

            if (transaction.isAborted()) {
                log.info(
                        "Transaction with timestamp {} was aborted",
                        SafeArg.of("startTimestamp", transaction.getTimestamp()));
                return Optional.empty();
            }

            log.info(
                    "Transaction with timestamp {} has completed successfully",
                    SafeArg.of("startTimestamp", transaction.getTimestamp()));
            return Optional.of(FullyWitnessedTransaction.builder()
                    .startTimestamp(transaction.getTimestamp())
                    .commitTimestamp(commitTimestampProvider
                            .get()
                            .getCommitTimestampOrThrowIfMaybeNotCommitted(transaction.getTimestamp()))
                    .actions(witnessedActionsReference.get())
                    .build());
        } catch (SafeIllegalArgumentException e) {
            throw e;
        } catch (TransactionCommitFailedException e) {
            Transaction transaction = transactionReference.get();
            return Optional.of(MaybeWitnessedTransaction.builder()
                    .startTimestamp(transaction.getTimestamp())
                    .commitTimestamp(commitTimestampProvider
                            .get()
                            .getCommitTimestampOrThrowIfMaybeNotCommitted(transaction.getTimestamp()))
                    .actions(witnessedActionsReference.get())
                    .build());
        } catch (Exception e) {
            Optional<Long> startTimestamp =
                    Optional.ofNullable(transactionReference.get()).map(Transaction::getTimestamp);
            log.info(
                    "Failed to record transaction due to an exception for startTimestamp {}",
                    SafeArg.of("startTimestamp", startTimestamp),
                    e);
            return Optional.empty();
        }
    }

    private static class AtlasDbTransactionActionVisitor implements TransactionActionVisitor<Void> {

        private final InteractiveTransaction transaction;

        public AtlasDbTransactionActionVisitor(InteractiveTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public Void visit(SingleCellReadTransactionAction singleCellReadTransactionAction) {
            transaction.read(singleCellReadTransactionAction.table(), singleCellReadTransactionAction.cell());
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

        @Override
        public Void visit(RowColumnRangeReadTransactionAction rowColumnRangeReadTransactionAction) {
            transaction.getRowColumnRange(
                    rowColumnRangeReadTransactionAction.table(),
                    rowColumnRangeReadTransactionAction.row(),
                    rowColumnRangeReadTransactionAction.columnRangeSelection());
            return null;
        }
    }

    @VisibleForTesting
    @SuppressWarnings("VisibleForTestingPackagePrivate")
    public static AtlasDbTransactionStore create(
            TransactionManager transactionManager, Map<TableReference, byte[]> tables) {
        return create(transactionManager, tables, () -> {});
    }

    public static AtlasDbTransactionStore create(
            TransactionManager transactionManager, Map<TableReference, byte[]> tables, Runnable preTransactionTask) {
        transactionManager.getKeyValueService().createTables(tables);
        Map<String, TableReference> tableMapping = EntryStream.of(tables)
                .keys()
                .mapToEntry(TableReference::getTableName, Function.identity())
                .toMap();
        return new AtlasDbTransactionStore(transactionManager, tableMapping, preTransactionTask);
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
