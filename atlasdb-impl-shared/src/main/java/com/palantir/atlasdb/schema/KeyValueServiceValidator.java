/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.schema;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageLevel;
import com.palantir.atlasdb.schema.KeyValueServiceMigrator.KvsMigrationMessageProcessor;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class KeyValueServiceValidator {
    private final TransactionManager validationFromTransactionManager;
    private final TransactionManager validationToTransactionManager;
    private final KeyValueService validationFromKvs;

    private final int threads;
    private final int defaultBatchSize;

    // Tables that exist on the legacy KVS and should not be migrated.
    // TODO(tgordeeva): hacky, clean this up when we have table specific migration
    private final Set<TableReference> unmigratableTables;

    private final Map<TableReference, Integer> readBatchSizeOverrides;

    private final KvsMigrationMessageProcessor messageProcessor;

    private final AtomicInteger migratedTableCount;

    public KeyValueServiceValidator(
            TransactionManager validationFromTransactionManager,
            TransactionManager validationToTransactionManager,
            KeyValueService validationFromKvs,
            int threads,
            int defaultBatchSize,
            Map<TableReference, Integer> readBatchSizeOverrides,
            KvsMigrationMessageProcessor messageProcessor,
            Set<TableReference> unmigratableTables,
            AtomicInteger migratedTableCount) {
        this.validationFromTransactionManager = validationFromTransactionManager;
        this.validationToTransactionManager = validationToTransactionManager;
        this.validationFromKvs = validationFromKvs;
        this.threads = threads;
        this.defaultBatchSize = defaultBatchSize;
        this.readBatchSizeOverrides = readBatchSizeOverrides;
        this.messageProcessor = messageProcessor;
        this.unmigratableTables = unmigratableTables;
        this.migratedTableCount = migratedTableCount;
    }

    private int getBatchSize(TableReference table) {
        Integer batchSize = readBatchSizeOverrides.get(table);
        return batchSize != null ? batchSize : defaultBatchSize;
    }

    public void validate(boolean logOnly) {
        Set<TableReference> tables =
                KeyValueServiceValidators.getValidatableTableNames(validationFromKvs, unmigratableTables);
        try {
            validateTables(tables);
        } catch (Throwable t) {
            KeyValueServiceMigratorUtils.processMessage(
                    messageProcessor, "Validation failed.", t, KvsMigrationMessageLevel.ERROR);
            if (!logOnly) {
                throw Throwables.throwUncheckedException(t);
            }
        }
    }

    private void validateTables(Set<TableReference> tables) {
        ExecutorService executor = PTExecutors.newFixedThreadPool(threads);
        List<Future<Void>> futures = new ArrayList<>();
        for (final TableReference table : tables) {
            Future<Void> future = executor.submit(() -> {
                try {
                    validateTable(table);
                    KeyValueServiceMigratorUtils.processMessage(
                            messageProcessor,
                            "Validated a table {} ({} of {})",
                            KvsMigrationMessageLevel.INFO,
                            LoggingArgs.tableRef(table),
                            SafeArg.of("migratedTableCount", migratedTableCount.incrementAndGet()),
                            SafeArg.of("totalTables", tables.size()));
                } catch (RuntimeException e) {
                    throw Throwables.rewrapAndThrowUncheckedException("Exception while validating " + table, e);
                }
                return null;
            });
            futures.add(future);
        }

        futures.forEach(Futures::getUnchecked);
        KeyValueServiceMigratorUtils.processMessage(
                messageProcessor, "Validation complete.", KvsMigrationMessageLevel.INFO);
    }

    private void validateTable(final TableReference table) {
        final int limit = getBatchSize(table);
        // read only, but need to use a write tx in case the source table has SweepStrategy.THOROUGH
        byte[] nextRowName = new byte[0];
        while (nextRowName != null) {
            nextRowName = validateNextBatchOfRows(table, limit, nextRowName);
            KeyValueServiceMigratorUtils.processMessage(
                    messageProcessor,
                    "Validated a batch of rows for {}",
                    KvsMigrationMessageLevel.INFO,
                    LoggingArgs.tableRef(table));
        }
    }

    private byte[] validateNextBatchOfRows(TableReference table, int limit, byte[] nextRowName) {
        try {
            // read only, but need to use a write tx in case the source table has SweepStrategy.THOROUGH
            return validationFromTransactionManager.runTaskWithRetry(
                    t1 -> validationToTransactionManager.runTaskWithRetry(t2 -> {
                        RangeRequest range = RangeRequest.builder()
                                .batchHint(limit)
                                .startRowInclusive(nextRowName)
                                .build();
                        return validateAndGetNextRowName(table, limit, t1, t2, range);
                    }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] validateAndGetNextRowName(
            TableReference table, int limit, Transaction t1, Transaction t2, RangeRequest range) {
        BatchingVisitableView<RowResult<byte[]>> bv1 = BatchingVisitableView.of(t1.getRange(table, range));
        List<RowResult<byte[]>> rrs1 = bv1.limit(limit).immutableCopy();
        Map<Cell, byte[]> cells1 = Cells.convertRowResultsToCells(rrs1);

        BatchingVisitableView<RowResult<byte[]>> bv2 = BatchingVisitableView.of(t2.getRange(table, range));
        List<RowResult<byte[]>> rrs2 = bv2.limit(limit).immutableCopy();
        Map<Cell, byte[]> cells2 = Cells.convertRowResultsToCells(rrs2);

        validateEquality(cells1, cells2);

        if (rrs1.isEmpty()) {
            return null;
        }

        byte[] lastRow = rrs1.get(rrs1.size() - 1).getRowName();
        if (RangeRequests.isLastRowName(lastRow)) {
            return null;
        }
        return RangeRequests.nextLexicographicName(lastRow);
    }

    private void validateEquality(Map<Cell, byte[]> cells1, Map<Cell, byte[]> cells2) {
        Set<Cell> ks1 = cells1.keySet();
        Set<Cell> ks2 = cells2.keySet();
        Preconditions.checkArgument(ks1.equals(ks2), "Cells not equal. Expected: %s. Actual: %s", ks1, ks2);
        for (Cell c : ks1) {
            Preconditions.checkArgument(Arrays.equals(cells1.get(c), cells2.get(c)), "Values not equal for cell %s", c);
        }
    }
}
