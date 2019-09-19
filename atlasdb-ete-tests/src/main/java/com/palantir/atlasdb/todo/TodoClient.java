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
package com.palantir.atlasdb.todo;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.sweep.ImmutableSweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepBatchConfig;
import com.palantir.atlasdb.sweep.SweepTaskRunner;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SpecialTimestampsSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.todo.generated.LatestSnapshotTable;
import com.palantir.atlasdb.todo.generated.NamespacedTodoTable;
import com.palantir.atlasdb.todo.generated.SnapshotsStreamStore;
import com.palantir.atlasdb.todo.generated.TodoSchemaTableFactory;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.ClosableIterator;
import com.palantir.util.Pair;
import com.palantir.util.crypto.Sha256Hash;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TodoClient {
    private static final Logger log = LoggerFactory.getLogger(TodoClient.class);

    private final TransactionManager transactionManager;
    private final Supplier<KeyValueService> kvs;
    private final Supplier<SweepTaskRunner> sweepTaskRunner;
    private final Supplier<TargetedSweeper> targetedSweeper;
    private final SpecialTimestampsSupplier sweepTimestampProvider;
    private final Random random = new Random();

    public TodoClient(TransactionManager transactionManager, Supplier<SweepTaskRunner> sweepTaskRunner,
            Supplier<TargetedSweeper> targetedSweeper) {
        this.transactionManager = transactionManager;
        this.kvs = Suppliers.memoize(transactionManager::getKeyValueService);
        this.sweepTaskRunner = sweepTaskRunner;
        this.targetedSweeper = targetedSweeper;
        // Intentionally providing the immutable timestamp instead of unreadable to avoid the delay
        this.sweepTimestampProvider = new SpecialTimestampsSupplier(transactionManager::getImmutableTimestamp,
                transactionManager::getImmutableTimestamp);
    }

    public void addTodo(Todo todo) {
        addTodoWithIdAndReturnTimestamp(random.nextLong(), todo);
    }

    public long addTodoWithIdAndReturnTimestamp(long id, Todo todo) {
        return transactionManager.runTaskWithRetry((transaction) -> {
            Cell thisCell = Cell.create(ValueType.FIXED_LONG.convertFromJava(id),
                    TodoSchema.todoTextColumn());
            Map<Cell, byte[]> write = ImmutableMap.of(thisCell, ValueType.STRING.convertFromJava(todo.text()));

            transaction.put(TodoSchema.todoTable(), write);
            return transaction.getTimestamp();
        });
    }

    public boolean doesNotExistBeforeTimestamp(long id, long ts) {
        TableReference tableRef = TodoSchemaTableFactory.of().getTodoTable(null).getTableRef();
        Cell cell = Cell.create(ValueType.FIXED_LONG.convertFromJava(id), TodoSchema.todoTextColumn());
        return kvs.get().get(tableRef, ImmutableMap.of(cell, ts + 1)).isEmpty();
    }

    public List<Todo> getTodoList() {
        ImmutableList<RowResult<byte[]>> results = transactionManager.runTaskWithRetry((transaction) -> {
            BatchingVisitable<RowResult<byte[]>> rowResultBatchingVisitable = transaction.getRange(
                    TodoSchema.todoTable(), RangeRequest.all());
            ImmutableList.Builder<RowResult<byte[]>> rowResults = ImmutableList.builder();

            rowResultBatchingVisitable.batchAccept(1000, items -> {
                rowResults.addAll(items);
                return true;
            });

            return rowResults.build();
        });

        return results.stream()
                .map(RowResult::getOnlyColumnValue)
                .map(ValueType.STRING::convertToString)
                .map(ImmutableTodo::of)
                .collect(Collectors.toList());
    }

    // Stores a new snapshot, marking the previous one as unused. We thus have at most one used stream at any time
    public void storeSnapshot(InputStream snapshot) {
        byte[] streamReference = "EteTest".getBytes();

        TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
        SnapshotsStreamStore streamStore = SnapshotsStreamStore.of(transactionManager, tableFactory);
        Long newStreamId = storeStreamAndGetId(snapshot, streamStore);

        transactionManager.runTaskWithRetry(transaction -> {
            // Load previous stream, and unmark it as used
            LatestSnapshotTable.LatestSnapshotRow row = LatestSnapshotTable.LatestSnapshotRow.of(0L);
            LatestSnapshotTable latestSnapshotTable = tableFactory.getLatestSnapshotTable(transaction);
            Optional<LatestSnapshotTable.LatestSnapshotRowResult> maybeRow = latestSnapshotTable.getRow(row);
            maybeRow.ifPresent(latestSnapshot -> {
                Long latestStreamId = maybeRow.get().getStreamId();

                log.info("Marking stream {}, ref {}, as unused", latestStreamId, PtBytes.toString(streamReference));
                Map<Long, byte[]> theMap = ImmutableMap.of(latestStreamId, streamReference);
                streamStore.unmarkStreamsAsUsed(transaction, theMap);
            });

            streamStore.markStreamAsUsed(transaction, newStreamId, streamReference);
            log.info("Marked stream {} as used with reference {}", newStreamId, PtBytes.toString(streamReference));

            // Record the latest snapshot
            latestSnapshotTable.putStreamId(row, newStreamId);

            return null;
        });
    }

    private Long storeStreamAndGetId(InputStream snapshot, SnapshotsStreamStore streamStore) {
        log.info("Storing stream...");
        Pair<Long, Sha256Hash> storedStream = streamStore.storeStream(snapshot);
        Long newStreamId = storedStream.getLhSide();
        log.info("Stored stream with ID {}", newStreamId);
        return newStreamId;
    }

    public void storeUnmarkedSnapshot(InputStream snapshot) {
        TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
        SnapshotsStreamStore streamStore = SnapshotsStreamStore.of(transactionManager, tableFactory);
        storeStreamAndGetId(snapshot, streamStore);
    }

    public void runIterationOfTargetedSweep() {
        runIterationOfTargetedSweepForShard(ShardAndStrategy.conservative(0));
        runIterationOfTargetedSweepForShard(ShardAndStrategy.thorough(0));
    }

    private void runIterationOfTargetedSweepForShard(ShardAndStrategy shardStrategy) {
        targetedSweeper.get()
                .sweepNextBatch(shardStrategy, Sweeper.of(shardStrategy).getSweepTimestamp(sweepTimestampProvider));
    }


    public SweepResults sweepSnapshotIndices() {
        TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
        TableReference indexTable = tableFactory.getSnapshotsStreamIdxTable(null).getTableRef();
        return sweepTable(indexTable);
    }

    public SweepResults sweepSnapshotValues() {
        TodoSchemaTableFactory tableFactory = TodoSchemaTableFactory.of(Namespace.DEFAULT_NAMESPACE);
        TableReference valueTable = tableFactory.getSnapshotsStreamValueTable(null).getTableRef();
        return sweepTable(valueTable);
    }

    SweepResults sweepTable(TableReference table) {
        SweepBatchConfig sweepConfig = ImmutableSweepBatchConfig.builder()
                .candidateBatchSize(AtlasDbConstants.DEFAULT_SWEEP_CANDIDATE_BATCH_HINT)
                .deleteBatchSize(AtlasDbConstants.DEFAULT_SWEEP_DELETE_BATCH_HINT)
                .maxCellTsPairsToExamine(AtlasDbConstants.DEFAULT_SWEEP_READ_LIMIT)
                .build();
        return sweepTaskRunner.get().run(table, sweepConfig, PtBytes.EMPTY_BYTE_ARRAY);
    }

    public long numAtlasDeletes(TableReference tableRef) {
        return getAllAtlasDeletes(tableRef).size();
    }

    public long numSweptAtlasDeletes(TableReference tableRef) {
        return getAllAtlasDeletes(tableRef).entrySet().stream()
                .filter(entry -> getValueForEntry(tableRef, entry).getTimestamp() == Value.INVALID_VALUE_TIMESTAMP)
                .count();
    }

    private Map<Cell, Value> getAllAtlasDeletes(TableReference tableRef) {
        Set<Cell> allCells = getAllCells(tableRef);
        Map<Cell, Value> latest = kvs.get().get(tableRef, Maps.asMap(allCells, ignore -> Long.MAX_VALUE));
        return Maps.filterEntries(latest, ent -> Arrays.equals(ent.getValue().getContents(), PtBytes.EMPTY_BYTE_ARRAY));
    }

    private Value getValueForEntry(TableReference tableRef, Map.Entry<Cell, Value> entry) {
        return kvs.get().get(tableRef, ImmutableMap.of(entry.getKey(), entry.getValue().getTimestamp()))
                .get(entry.getKey());
    }

    private Set<Cell> getAllCells(TableReference tableRef) {
        try (ClosableIterator<RowResult<Value>> iterator = kvs.get()
                .getRange(tableRef, RangeRequest.all(), Long.MAX_VALUE)) {
            return iterator.stream()
                    .map(RowResult::getCells)
                    .flatMap(Streams::stream)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
        }
    }

    public void truncate() {
        Schemas.truncateTablesAndIndexes(TodoSchema.getSchema(), kvs.get());
        Schemas.truncateTablesAndIndexes(TargetedSweepSchema.schemaWithoutTableIdentifierTables(), kvs.get());
    }

    public long addNamespacedTodoWithIdAndReturnTimestamp(long id, String namespace, Todo todo) {
        return transactionManager.runTaskWithRetry(tx -> {
            TodoSchemaTableFactory.of().getNamespacedTodoTable(tx).put(
                    NamespacedTodoTable.NamespacedTodoRow.of(namespace),
                    NamespacedTodoTable.NamespacedTodoColumnValue.of(
                            NamespacedTodoTable.NamespacedTodoColumn.of(id),
                            todo.text()));
            return tx.getTimestamp();
        });
    }

    public boolean namespacedTodoDoesNotExistBeforeTimestamp(long id, long timestamp, String namespace) {
        return kvs.get().get(TodoSchema.namespacedTodoTable(),
                ImmutableMap.of(Cell.create(PtBytes.toBytes(namespace), PtBytes.toBytes(id)), timestamp + 1)).isEmpty();
    }
}
