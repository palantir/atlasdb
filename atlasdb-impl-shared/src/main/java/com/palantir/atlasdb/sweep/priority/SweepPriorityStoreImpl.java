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
package com.palantir.atlasdb.sweep.priority;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityNamedColumn;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRow;
import com.palantir.atlasdb.schema.generated.SweepPriorityTable.SweepPriorityRowResult;
import com.palantir.atlasdb.schema.generated.SweepTableFactory;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.impl.UnmodifiableTransaction;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

public final class SweepPriorityStoreImpl implements SweepPriorityStore {
    private final class InitializingWrapper extends AsyncInitializer implements AutoDelegate_SweepPriorityStore {

        @Override
        public SweepPriorityStoreImpl delegate() {
            checkInitialized();
            return SweepPriorityStoreImpl.this;
        }

        @Override
        protected void tryInitialize() {
            SweepPriorityStoreImpl.this.tryInitialize();
        }

        @Override
        protected String getInitializingClassName() {
            return "SweepPriorityStore";
        }
    }

    private static final int READ_BATCH_SIZE = 100;

    private final KeyValueService kvs;
    private final SweepTableFactory sweepTableFactory;
    private InitializingWrapper wrapper = new InitializingWrapper();

    private SweepPriorityStoreImpl(KeyValueService kvs, SweepTableFactory sweepTableFactory) {
        this.kvs = kvs;
        this.sweepTableFactory = sweepTableFactory;
    }

    public static SweepPriorityStore create(
            KeyValueService kvs, SweepTableFactory sweepTableFactory, boolean initializeAsync) {
        SweepPriorityStoreImpl sweepPriorityStore = new SweepPriorityStoreImpl(kvs, sweepTableFactory);
        sweepPriorityStore.wrapper.initialize(initializeAsync);
        return sweepPriorityStore.wrapper.isInitialized() ? sweepPriorityStore : sweepPriorityStore.wrapper;
    }

    @Override
    public List<SweepPriority> loadOldPriorities(Transaction tx, long sweepTimestamp) {
        return loadPriorities(new SweepPriorityTransaction(tx, sweepTimestamp));
    }

    @Override
    public List<SweepPriority> loadNewPriorities(Transaction tx) {
        return loadPriorities(tx);
    }

    @Override
    public void update(Transaction tx, TableReference tableRef, UpdateSweepPriority update) {
        SweepPriorityRow row = SweepPriorityRow.of(tableRef.getQualifiedName());
        SweepPriorityTable table = sweepTableFactory.getSweepPriorityTable(tx);
        update.newStaleValuesDeleted().ifPresent(n -> table.putCellsDeleted(row, n));
        update.newCellTsPairsExamined().ifPresent(n -> table.putCellsExamined(row, n));
        update.newLastSweepTimeMillis().ifPresent(t -> table.putLastSweepTime(row, t));
        update.newMinimumSweptTimestamp().ifPresent(t -> table.putMinimumSweptTimestamp(row, t));
        update.newWriteCount().ifPresent(c -> table.putWriteCount(row, c));
    }

    @Override
    public void delete(Transaction tx, Collection<TableReference> tableRefs) {
        sweepTableFactory
                .getSweepPriorityTable(tx)
                .delete(Collections2.transform(tableRefs, tr -> SweepPriorityRow.of(tr.getQualifiedName())));
    }

    private void tryInitialize() {
        Schemas.createTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private List<SweepPriority> loadPriorities(Transaction tx) {
        SweepPriorityTable table = sweepTableFactory.getSweepPriorityTable(tx);

        // Load a single column first for each row. This is a much more efficient query on Cassandra
        // than the full table scan that occurs otherwise.
        List<SweepPriorityRowResult> rows = table.getRange(RangeRequest.builder()
                        .retainColumns(SweepPriorityTable.getColumnSelection(SweepPriorityNamedColumn.CELLS_DELETED))
                        .batchHint(READ_BATCH_SIZE)
                        .build())
                .immutableCopy();

        // Fetch all columns for the above rows directly
        return Lists.transform(
                table.getRows(Lists.transform(rows, SweepPriorityRowResult::getRowName)),
                SweepPriorityStoreImpl::hydrate);
    }

    private static SweepPriority hydrate(SweepPriorityTable.SweepPriorityRowResult rr) {
        return ImmutableSweepPriority.builder()
                .tableRef(TableReference.createUnsafe(rr.getRowName().getFullTableName()))
                .writeCount(rr.hasWriteCount() ? rr.getWriteCount() : 0L)
                .lastSweepTimeMillis(
                        rr.hasLastSweepTime() ? OptionalLong.of(rr.getLastSweepTime()) : OptionalLong.empty())
                .minimumSweptTimestamp(rr.hasMinimumSweptTimestamp() ? rr.getMinimumSweptTimestamp() : Long.MIN_VALUE)
                .staleValuesDeleted(rr.hasCellsDeleted() ? rr.getCellsDeleted() : 0L)
                .cellTsPairsExamined(rr.hasCellsExamined() ? rr.getCellsExamined() : 0L)
                .build();
    }

    // I didn't write this ****, only moved it from another file.
    // This has never worked as intended.
    // We probably need to completely redesign how we store historical priorities.
    private static class SweepPriorityTransaction extends UnmodifiableTransaction {
        private final long sweepTimestamp;

        SweepPriorityTransaction(Transaction delegate, long sweepTimestamp) {
            super(delegate);
            this.sweepTimestamp = sweepTimestamp;
        }

        @Override
        public long getTimestamp() {
            return sweepTimestamp;
        }

        @Override
        public TransactionReadSentinelBehavior getReadSentinelBehavior() {
            return TransactionReadSentinelBehavior.IGNORE;
        }
    }
}
