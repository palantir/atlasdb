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

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.util.AtlasDbUtils;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import one.util.streamex.EntryStream;

public final class AtlasDbTransactionStore implements TransactionStore {

    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbTransactionStore.class);
    private final TransactionManager transactionManager;

    private final Map<String, TableReference> tables;

    private AtlasDbTransactionStore(TransactionManager transactionManager, Map<String, TableReference> tables) {
        this.transactionManager = transactionManager;
        this.tables = tables;
    }

    @Override
    public Optional<Integer> get(String table, WorkloadCell cell) {
        Cell atlasCell = AtlasDbUtils.toAtlasCell(cell);
        TableReference tableReference = getTableReferenceOrThrow(table);
        return transactionManager.runTaskWithRetry(task -> {
            Map<Cell, byte[]> values = task.get(tableReference, Set.of(atlasCell));
            return Optional.ofNullable(values.get(atlasCell)).map(Ints::fromByteArray);
        });
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions) {
        AtomicReference<Long> startTimestampReference = new AtomicReference<>();
        AtomicReference<List<WitnessedTransactionAction>> witnessedActionsReference = new AtomicReference<>();
        try {
            transactionManager.runTaskWithRetry(txn -> {
                AtlasDbTransactionActionVisitor visitor = new AtlasDbTransactionActionVisitor(txn);
                startTimestampReference.set(txn.getTimestamp());
                witnessedActionsReference.set(actions.stream()
                        .sequential()
                        .map(action -> action.accept(visitor))
                        .collect(Collectors.toList()));
                return null;
            });

            TransactionStatus status =
                    transactionManager.getTransactionService().getV2(startTimestampReference.get());

            Optional<Long> commitTimestamp = TransactionStatus.getCommitTimestamp(status);

            return Optional.of(ImmutableWitnessedTransaction.builder()
                    .startTimestamp(startTimestampReference.get())
                    .commitTimestamp(commitTimestamp)
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

    private TableReference getTableReferenceOrThrow(TransactionAction action) {
        return getTableReferenceOrThrow(action.table());
    }

    private TableReference getTableReferenceOrThrow(String table) {
        return Optional.ofNullable(tables.get(table))
                .orElseThrow(() -> new SafeIllegalArgumentException(
                        "Transaction action has unknown table.",
                        SafeArg.of("readTransactionTableName", table),
                        SafeArg.of("availableTables", tables)));
    }

    private class AtlasDbTransactionActionVisitor implements TransactionActionVisitor<WitnessedTransactionAction> {

        private final Transaction transaction;

        public AtlasDbTransactionActionVisitor(Transaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public WitnessedTransactionAction visit(ReadTransactionAction readTransactionAction) {
            Cell cell = AtlasDbUtils.toAtlasCell(readTransactionAction.cell());
            TableReference tableReference = getTableReferenceOrThrow(readTransactionAction);
            Map<Cell, byte[]> cells =
                    transaction.get(tableReference, Set.of(AtlasDbUtils.toAtlasCell(readTransactionAction.cell())));
            Optional<Integer> value = Optional.ofNullable(cells.get(cell)).map(Ints::fromByteArray);
            return readTransactionAction.witness(value);
        }

        @Override
        public WitnessedTransactionAction visit(WriteTransactionAction writeTransactionAction) {
            TableReference tableReference = getTableReferenceOrThrow(writeTransactionAction);
            transaction.put(
                    tableReference,
                    Map.of(
                            AtlasDbUtils.toAtlasCell(writeTransactionAction.cell()),
                            Ints.toByteArray(writeTransactionAction.value())));
            return writeTransactionAction.witness();
        }

        @Override
        public WitnessedTransactionAction visit(DeleteTransactionAction deleteTransactionAction) {
            TableReference tableReference = getTableReferenceOrThrow(deleteTransactionAction);
            transaction.delete(tableReference, Set.of(AtlasDbUtils.toAtlasCell(deleteTransactionAction.cell())));
            return deleteTransactionAction.witness();
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
}
