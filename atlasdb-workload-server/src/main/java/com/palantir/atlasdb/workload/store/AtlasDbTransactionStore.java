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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
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
import com.palantir.atlasdb.workload.transaction.*;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AtlasDbTransactionStore implements TransactionStore {

    private final TransactionManager transactionManager;
    private final TableReference tableReference;

    private AtlasDbTransactionStore(TransactionManager transactionManager, TableReference tableReference) {
        this.transactionManager = transactionManager;
        this.tableReference = tableReference;
    }

    @Override
    public Optional<WorkloadCell> get(Integer key, Integer column) {
        transactionManager.runTaskWithRetry(task -> task.get(
                tableReference, Set.of(Cell.create(Ints.toByteArray(key.intValue()), Ints.toByteArray(column)))));
    }

    @Override
    public Optional<WorkloadCell> get(Integer key) {
        return Optional.empty();
    }

    @Override
    public Optional<WitnessedTransaction> readWrite(List<TransactionAction> actions) {
        try {
            AtomicReference<Long> transactionReference = new AtomicReference<>();
            List<TransactionAction> executedActions = transactionManager.runTaskWithRetry(txn -> {
                AtlasDbTransactionActionVisitor visitor = new AtlasDbTransactionActionVisitor(txn);
                transactionReference.set(txn.getTimestamp());
                return actions.stream()
                        .sequential()
                        .map(action -> action.accept(visitor))
                        .collect(Collectors.toList());
            });
            TransactionStatus status =
                    transactionManager.getTransactionService().getV2(transactionReference.get());
            long commitTimestamp =
                    TransactionStatuses.getCommitTimestamp(status).orElseThrow();
            return WitnessedTransaction.builder().build();
        } catch (Exception e) {
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
            Map<Cell, byte[]> cells = transaction.get(tableReference, Set.of());
            Integer value = Ints.fromByteArray(Iterables.getOnlyElement(cells.values()));
            return readTransactionAction.record(value);
        }

        @Override
        public TransactionAction visit(HistoricalReadTransactionAction historicalReadTransactionAction) {
            throw new SafeIllegalStateException("This should never be possible.");
        }

        @Override
        public TransactionAction visit(WriteTransactionAction writeTransactionAction) {
            transaction.put(tableReference, Map.of());
            return writeTransactionAction;
        }

        @Override
        public TransactionAction visit(DeleteTransactionAction deleteTransactionAction) {
            return null;
        }
    }

    public static AtlasDbTransactionStore create(
            TransactionManager transactionManager, TableReference tableReference, ConflictHandler conflictHandler) {
        transactionManager.getKeyValueService().createTable(tableReference, metadata(conflictHandler));
        return new AtlasDbTransactionStore(transactionManager, tableReference);
    }

    public static TransactionManager createTransactionManager(
            AtlasDbConfig config, Refreshable<Optional<AtlasDbRuntimeConfig>> runtimeConfig) {
        return TransactionManagers.builder()
                .config(config)
                .userAgent(UserAgent.of(UserAgent.Agent.of("atlasdb-workload" + "-server", "0.0.1")))
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(new DefaultTaggedMetricRegistry())
                .runtimeConfig(runtimeConfig)
                .build()
                .serializable();
    }

    private static byte[] metadata(ConflictHandler conflictHandler) {
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
}
