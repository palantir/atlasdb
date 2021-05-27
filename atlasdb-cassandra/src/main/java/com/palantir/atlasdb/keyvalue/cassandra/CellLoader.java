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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.TaskRunner.KvsLoadingTask;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.tracing.CloseableTracer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyPredicate;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CellLoader {
    private static final Logger log = LoggerFactory.getLogger(CellLoader.class);

    private final CassandraClientPool clientPool;
    private final WrappingQueryRunner queryRunner;
    private final TaskRunner taskRunner;
    private final CellLoadingBatcher batcher;

    private CellLoader(
            CassandraClientPool clientPool,
            WrappingQueryRunner queryRunner,
            TaskRunner taskRunner,
            CellLoadingBatcher batcher) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.taskRunner = taskRunner;
        this.batcher = batcher;
    }

    static CellLoader create(
            CassandraClientPool clientPool,
            WrappingQueryRunner queryRunner,
            TaskRunner taskRunner,
            Supplier<CassandraKeyValueServiceRuntimeConfig> configSupplier) {
        CellLoadingBatcher batcher = new CellLoadingBatcher(
                () -> configSupplier.get().cellLoadingConfig(), CellLoader::logRebatchingWarnMessage);
        return new CellLoader(clientPool, queryRunner, taskRunner, batcher);
    }

    Multimap<Cell, Long> getAllTimestamps(
            TableReference tableRef, Set<Cell> cells, long ts, ConsistencyLevel consistency) {
        CassandraKeyValueServices.AllTimestampsCollector collector =
                new CassandraKeyValueServices.AllTimestampsCollector();
        loadWithTs("getAllTimestamps", tableRef, cells, ts, true, collector, consistency);
        return collector.getCollectedResults();
    }

    void loadWithTs(
            String kvsMethodName,
            TableReference tableRef,
            Set<Cell> cells,
            long startTs,
            boolean loadAllTs,
            CassandraKeyValueServices.ThreadSafeResultVisitor visitor,
            ConsistencyLevel consistency) {
        Map<InetSocketAddress, List<Cell>> hostsAndCells;
        try (CloseableTracer tracer = CloseableTracer.startSpan(
                "partitionByHost",
                ImmutableMap.of(
                        "cells",
                        String.valueOf(cells.size()),
                        "tableRef",
                        LoggingArgs.safeInternalTableNameOrPlaceholder(tableRef.toString()),
                        "timestampClause",
                        loadAllTs ? "for all timestamps " : "",
                        "startTs",
                        String.valueOf(startTs)))) {
            hostsAndCells = HostPartitioner.partitionByHost(clientPool, cells, Cell::getRowName);
        }
        int totalPartitions = hostsAndCells.keySet().size();

        if (log.isTraceEnabled()) {
            log.trace(
                    "Loading {} cells from {} {}starting at timestamp {}, partitioned across {} nodes.",
                    SafeArg.of("cells", cells.size()),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("timestampClause", loadAllTs ? "for all timestamps " : ""),
                    SafeArg.of("startTs", startTs),
                    SafeArg.of("totalPartitions", totalPartitions));
        }

        List<KvsLoadingTask<Void>> tasks = new ArrayList<>();
        for (Map.Entry<InetSocketAddress, List<Cell>> hostAndCells : hostsAndCells.entrySet()) {
            if (log.isTraceEnabled()) {
                log.trace(
                        "Requesting {} cells from {} {}starting at timestamp {} on {}",
                        SafeArg.of("cells", hostsAndCells.values().size()),
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("timestampClause", loadAllTs ? "for all timestamps " : ""),
                        SafeArg.of("startTs", startTs),
                        SafeArg.of("ipPort", hostAndCells.getKey()));
            }

            try (CloseableTracer tracer = CloseableTracer.startSpan("getLoadWithTsTasksForSingleHost")) {
                tasks.addAll(getLoadWithTsTasksForSingleHost(
                        kvsMethodName,
                        hostAndCells.getKey(),
                        tableRef,
                        hostAndCells.getValue(),
                        startTs,
                        loadAllTs,
                        visitor,
                        consistency));
            }
        }

        taskRunner.runAllTasksCancelOnFailure(tasks);
    }

    // TODO(unknown): after cassandra api change: handle different column select per row
    private List<KvsLoadingTask<Void>> getLoadWithTsTasksForSingleHost(
            final String kvsMethodName,
            final InetSocketAddress host,
            final TableReference tableRef,
            final Collection<Cell> cells,
            final long startTs,
            final boolean loadAllTs,
            final CassandraKeyValueServices.ThreadSafeResultVisitor visitor,
            final ConsistencyLevel consistency) {
        final ColumnParent colFam = new ColumnParent(CassandraKeyValueServiceImpl.internalTableName(tableRef));
        List<KvsLoadingTask<Void>> tasks = new ArrayList<>();
        for (final List<Cell> partition : batcher.partitionIntoBatches(cells, host, tableRef)) {
            Callable<Void> multiGetCallable = () -> clientPool.runWithRetryOnHost(
                    host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
                        @Override
                        public Void apply(CassandraClient client) throws Exception {
                            List<KeyPredicate> query = translatePartitionToKeyPredicates(partition, startTs, loadAllTs);

                            if (log.isTraceEnabled()) {
                                log.trace(
                                        "Requesting {} cells from {} {}starting at timestamp {} on {}",
                                        SafeArg.of("cells", partition.size()),
                                        LoggingArgs.tableRef(tableRef),
                                        SafeArg.of("timestampClause", loadAllTs ? "for all timestamps " : ""),
                                        SafeArg.of("startTs", startTs),
                                        SafeArg.of("host", CassandraLogHelper.host(host)));
                            }

                            Map<ByteBuffer, List<List<ColumnOrSuperColumn>>> results = queryRunner.multiget_multislice(
                                    kvsMethodName, client, tableRef, query, consistency);
                            Map<ByteBuffer, List<ColumnOrSuperColumn>> aggregatedResults =
                                    Maps.transformValues(results, lists -> Lists.newArrayList(Iterables.concat(lists)));
                            visitor.visit(aggregatedResults);
                            return null;
                        }

                        @Override
                        public String toString() {
                            return "multiget_multislice(" + host + ", " + colFam + ", " + partition.size() + " cells"
                                    + ")";
                        }
                    });
            tasks.add(ImmutableCallableWithMetadata.of(
                    AnnotatedCallable.wrapWithThreadName(
                            AnnotationType.PREPEND,
                            "Atlas loadWithTs " + partition.size() + " cells from " + tableRef + " on " + host,
                            multiGetCallable),
                    ImmutableMetadata.builder()
                            .taskName("loadWithTs")
                            .numCells(partition.size())
                            .tableRefs(ImmutableSet.of(tableRef))
                            .host(host.getHostName())
                            .build()));
        }
        return tasks;
    }

    private static List<KeyPredicate> translatePartitionToKeyPredicates(
            List<Cell> partition, long startTs, boolean loadAllTs) {
        Map<byte[], SlicePredicate> canonicalPredicates = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        List<KeyPredicate> keyPredicates = new ArrayList<>(partition.size());

        for (Cell cell : partition) {
            SlicePredicate predicate = canonicalPredicates.computeIfAbsent(cell.getColumnName(), columnKey -> {
                SlicePredicates.Range range = SlicePredicates.Range.singleColumn(columnKey, startTs);
                SlicePredicates.Limit limit = loadAllTs ? SlicePredicates.Limit.NO_LIMIT : SlicePredicates.Limit.ONE;
                return SlicePredicates.create(range, limit);
            });

            KeyPredicate keyPredicate =
                    new KeyPredicate().setKey(cell.getRowName()).setPredicate(predicate);
            keyPredicates.add(keyPredicate);
        }
        return keyPredicates;
    }

    private static void logRebatchingWarnMessage(InetSocketAddress host, TableReference tableRef, int numRows) {
        log.warn(
                "Re-batching in getLoadWithTsTasksForSingleHost a call to {} for table {} that attempted to"
                        + " multiget {} rows; this may indicate overly-large batching on a higher level."
                        + " Note that batches are executed in parallel, which may cause load on both"
                        + " your Atlas client as well as on Cassandra if the number of rows is exceptionally"
                        + " high.\n{}",
                SafeArg.of("host", CassandraLogHelper.host(host)),
                LoggingArgs.tableRef(tableRef),
                SafeArg.of("rows", numRows),
                SafeArg.of("stacktrace", CassandraKeyValueServices.getFilteredStackTrace("com.palantir")));
    }
}
