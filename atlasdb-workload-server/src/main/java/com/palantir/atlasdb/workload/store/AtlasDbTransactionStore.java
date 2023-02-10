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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.atlasdb.workload.transaction.DeleteTransactionAction;
import com.palantir.atlasdb.workload.transaction.HistoricalReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.ImmutableWitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.ReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionAction;
import com.palantir.atlasdb.workload.transaction.TransactionActionVisitor;
import com.palantir.atlasdb.workload.transaction.WitnessedTransaction;
import com.palantir.atlasdb.workload.transaction.WriteTransactionAction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class AtlasDbTransactionStore implements TransactionStore {

    private static final SafeLogger log = SafeLoggerFactory.get(AtlasDbTransactionStore.class);
    private final TransactionManager transactionManager;
    private final TableReference tableReference;
    private final TableReference indexTableReference;

    private AtlasDbTransactionStore(
            TransactionManager transactionManager, TableReference tableReference, TableReference indexTableReference) {
        this.transactionManager = transactionManager;
        this.tableReference = tableReference;
        this.indexTableReference = indexTableReference;
    }

    @Override
    public Optional<Integer> get(WorkloadCell cell) {
        return transactionManager.runTaskWithRetry(task -> {
            Cell atlasCell = cell.toCell();
            Map<Cell, byte[]> values = task.get(tableReference, Set.of(cell.toCell()));
            return Optional.ofNullable(values.get(atlasCell)).map(Ints::fromByteArray);
        });
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions) {
        try {
            AtomicReference<Long> startTimestampReference = new AtomicReference<>();
            List<TransactionAction> executedActions = transactionManager.runTaskWithRetry(txn -> {
                AtlasDbTransactionActionVisitor visitor = new AtlasDbTransactionActionVisitor(txn);
                startTimestampReference.set(txn.getTimestamp());
                return actions.stream()
                        .sequential()
                        .map(action -> action.accept(visitor))
                        .collect(Collectors.toList());
            });

            TransactionStatus status =
                    transactionManager.getTransactionService().getV2(startTimestampReference.get());
            long commitTimestamp = TransactionStatuses.getCommitTimestamp(status)
                    .orElseThrow(() -> new SafeIllegalStateException(
                            "Transaction reported that it had committed, despite that it has not."
                                    + " This should never happen.",
                            SafeArg.of("transactionStatus", status),
                            SafeArg.of("startTimestamp", startTimestampReference.get())));

            return Optional.of(ImmutableWitnessedTransaction.builder()
                    .startTimestamp(startTimestampReference.get())
                    .commitTimestamp(commitTimestamp)
                    .actions(executedActions)
                    .build());
        } catch (Exception e) {
            log.info("Failed to record transaction due to an exception", e);
            return Optional.empty();
        }
    }

    private class AtlasDbTransactionActionVisitor implements TransactionActionVisitor<TransactionAction> {

        private final Transaction transaction;

        public AtlasDbTransactionActionVisitor(Transaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public TransactionAction visit(ReadTransactionAction readTransactionAction) {
            Cell cell = readTransactionAction.cell().toCell();
            Map<Cell, byte[]> cells = transaction.get(
                    tableReference, Set.of(readTransactionAction.cell().toCell()));
            Optional<Integer> value = Optional.ofNullable(cells.get(cell)).map(Ints::fromByteArray);
            return readTransactionAction.record(value);
        }

        @Override
        public TransactionAction visit(HistoricalReadTransactionAction historicalReadTransactionAction) {
            throw new SafeIllegalStateException("This should never be possible.");
        }

        @Override
        public TransactionAction visit(WriteTransactionAction writeTransactionAction) {
            transaction.put(
                    tableReference,
                    Map.of(writeTransactionAction.cell().toCell(), Ints.toByteArray(writeTransactionAction.value())));
            transaction.put(
                    indexTableReference,
                    Map.of(
                            writeTransactionAction.indexCell().toCell(),
                            Ints.toByteArray(writeTransactionAction.indexValue())));
            return writeTransactionAction;
        }

        @Override
        public TransactionAction visit(DeleteTransactionAction deleteTransactionAction) {
            transaction.delete(
                    tableReference, Set.of(deleteTransactionAction.cell().toCell()));
            return deleteTransactionAction;
        }
    }

    public static AtlasDbTransactionStore create(
            TransactionManager transactionManager, TableReference table, ConflictHandler conflictHandler) {
        TableReference indexTable = TableReference.create(table.getNamespace(), table.getTableName() + "_index");
        transactionManager.getKeyValueService().createTable(table, tableMetadata(conflictHandler));
        transactionManager.getKeyValueService().createTable(indexTable, indexMetadata());
        return new AtlasDbTransactionStore(transactionManager, table, indexTable);
    }

    private static byte[] tableMetadata(ConflictHandler conflictHandler) {
        return new TableMetadata.Builder()
                .rowMetadata(new NameMetadataDescription())
                .columns(new ColumnMetadataDescription())
                .conflictHandler(conflictHandler)
                .cachePriority(TableMetadataPersistence.CachePriority.WARM)
                .rangeScanAllowed(true)
                .explicitCompressionBlockSizeKB(AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB)
                .negativeLookups(true)
                .sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
                .appendHeavyAndReadLight(false)
                .nameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                .build()
                .persistToBytes();
    }

    private static byte[] indexMetadata() {
        return new TableMetadata.Builder()
                .rowMetadata(new NameMetadataDescription())
                .columns(new ColumnMetadataDescription())
                .conflictHandler(ConflictHandler.SERIALIZABLE_INDEX)
                .cachePriority(TableMetadataPersistence.CachePriority.WARM)
                .rangeScanAllowed(true)
                .explicitCompressionBlockSizeKB(AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB)
                .negativeLookups(true)
                .sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
                .appendHeavyAndReadLight(false)
                .nameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                .build()
                .persistToBytes();
    }
}
