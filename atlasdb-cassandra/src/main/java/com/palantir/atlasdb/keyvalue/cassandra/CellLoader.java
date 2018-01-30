/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.logsafe.SafeArg;

class CellLoader {
    private static final Logger log = LoggerFactory.getLogger(CellLoader.class);

    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;
    private final WrappingQueryRunner queryRunner;
    private final TaskRunner taskRunner;

    CellLoader(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool, WrappingQueryRunner queryRunner,
            TaskRunner taskRunner) {
        this.config = config;
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.taskRunner = taskRunner;
    }

    void loadWithTs(String kvsMethodName,
            TableReference tableRef,
            Set<Cell> cells,
            long startTs,
            boolean loadAllTs,
            CassandraKeyValueServices.ThreadSafeResultVisitor visitor,
            ConsistencyLevel consistency) {
        Map<InetSocketAddress, List<Cell>> hostsAndCells = new HostPartitioner<>(clientPool).partitionByHost(
                cells, Cells.getRowFunction());
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

        List<Callable<Void>> tasks = Lists.newArrayList();
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

            tasks.addAll(getLoadWithTsTasksForSingleHost(kvsMethodName,
                    hostAndCells.getKey(),
                    tableRef,
                    hostAndCells.getValue(),
                    startTs,
                    loadAllTs,
                    visitor,
                    consistency));
        }

        taskRunner.runAllTasksCancelOnFailure(tasks);
    }

    // TODO(unknown): after cassandra api change: handle different column select per row
    private List<Callable<Void>> getLoadWithTsTasksForSingleHost(final String kvsMethodName,
            final InetSocketAddress host,
            final TableReference tableRef,
            final Collection<Cell> cells,
            final long startTs,
            final boolean loadAllTs,
            final CassandraKeyValueServices.ThreadSafeResultVisitor visitor,
            final ConsistencyLevel consistency) {
        final ColumnParent colFam = new ColumnParent(CassandraKeyValueServiceImpl.internalTableName(tableRef));
        Multimap<byte[], Cell> cellsByCol =
                TreeMultimap.create(UnsignedBytes.lexicographicalComparator(), Ordering.natural());
        for (Cell cell : cells) {
            cellsByCol.put(cell.getColumnName(), cell);
        }
        List<Callable<Void>> tasks = Lists.newArrayList();
        int fetchBatchCount = config.fetchBatchCount();
        for (Map.Entry<byte[], Collection<Cell>> entry : Multimaps.asMap(cellsByCol).entrySet()) {
            final byte[] col = entry.getKey();
            Collection<Cell> columnCells = entry.getValue();
            if (columnCells.size() > fetchBatchCount) {
                log.warn("Re-batching in getLoadWithTsTasksForSingleHost a call to {} for table {} that attempted to "
                                + "multiget {} rows; this may indicate overly-large batching on a higher level.\n{}",
                        SafeArg.of("host", CassandraLogHelper.host(host)),
                        LoggingArgs.tableRef(tableRef),
                        SafeArg.of("rows", columnCells.size()),
                        SafeArg.of("stacktrace", CassandraKeyValueServices.getFilteredStackTrace("com.palantir")));
            }
            for (final List<Cell> partition : Lists.partition(ImmutableList.copyOf(columnCells), fetchBatchCount)) {
                Callable<Void> multiGetCallable = () -> clientPool.runWithRetryOnHost(
                        host,
                        new FunctionCheckedException<CassandraClient, Void, Exception>() {
                            @Override
                            public Void apply(CassandraClient client) throws Exception {
                                SlicePredicates.Range range = SlicePredicates.Range.singleColumn(col, startTs);
                                SlicePredicates.Limit limit =
                                        loadAllTs ? SlicePredicates.Limit.NO_LIMIT : SlicePredicates.Limit.ONE;
                                SlicePredicate predicate = SlicePredicates.create(range, limit);

                                List<ByteBuffer> rowNames = Lists.newArrayListWithCapacity(partition.size());
                                for (Cell c : partition) {
                                    rowNames.add(ByteBuffer.wrap(c.getRowName()));
                                }

                                if (log.isTraceEnabled()) {
                                    log.trace("Requesting {} cells from {} {}starting at timestamp {} on {}",
                                            SafeArg.of("cells", partition.size()),
                                            LoggingArgs.tableRef(tableRef),
                                            SafeArg.of("timestampClause", loadAllTs ? "for all timestamps " : ""),
                                            SafeArg.of("startTs", startTs),
                                            SafeArg.of("host", CassandraLogHelper.host(host)));
                                }

                                Map<ByteBuffer, List<ColumnOrSuperColumn>> results = queryRunner.multiget(
                                        kvsMethodName, client, tableRef, rowNames, predicate, consistency);
                                visitor.visit(results);
                                return null;
                            }

                            @Override
                            public String toString() {
                                return "multiget_slice(" + host + ", " + colFam + ", "
                                        + partition.size() + " cells" + ")";
                            }

                        });
                tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                        "Atlas loadWithTs " + partition.size() + " cells from " + tableRef + " on " + host,
                        multiGetCallable));
            }
        }
        return tasks;
    }
}
