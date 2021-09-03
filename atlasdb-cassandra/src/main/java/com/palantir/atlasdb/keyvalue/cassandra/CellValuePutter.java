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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.IterablePartitioner;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.base.FunctionCheckedException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;

public class CellValuePutter {

    private final LongSupplier timestampOverrideSupplier;

    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;
    private final TaskRunner taskRunner;
    private final WrappingQueryRunner queryRunner;

    public CellValuePutter(
            CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            TaskRunner taskRunner,
            WrappingQueryRunner queryRunner,
            LongSupplier timestampOverrideSupplier) {
        this.config = config;
        this.clientPool = clientPool;
        this.taskRunner = taskRunner;
        this.queryRunner = queryRunner;
        this.timestampOverrideSupplier = timestampOverrideSupplier;
    }

    void putWithOverriddenTimestamps(
            final String kvsMethodName, final TableReference tableRef, final Iterable<Map.Entry<Cell, Value>> values) {
        put(kvsMethodName, tableRef, values, true);
    }

    void put(final String kvsMethodName, final TableReference tableRef, final Iterable<Map.Entry<Cell, Value>> values) {
        put(kvsMethodName, tableRef, values, false);
    }

    private void put(
            final String kvsMethodName,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values,
            boolean overwriteTimestamps) {
        Map<InetSocketAddress, Map<Cell, Value>> cellsByHost = HostPartitioner.partitionMapByHost(clientPool, values);
        List<Callable<Void>> tasks = new ArrayList<>(cellsByHost.size());
        for (final Map.Entry<InetSocketAddress, Map<Cell, Value>> entry : cellsByHost.entrySet()) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(
                    AnnotationType.PREPEND,
                    "Atlas put " + entry.getValue().size() + " cell values to " + tableRef + " on " + entry.getKey(),
                    () -> {
                        putForSingleHost(
                                kvsMethodName,
                                entry.getKey(),
                                tableRef,
                                entry.getValue().entrySet(),
                                overwriteTimestamps);
                        return null;
                    }));
        }
        taskRunner.runAllTasksCancelOnFailure(tasks);
    }

    private static Long getEntrySize(Map.Entry<Cell, Value> input) {
        return input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());
    }

    private void putForSingleHost(
            String kvsMethodName,
            final InetSocketAddress host,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values,
            boolean overrideTimestamps)
            throws Exception {
        clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
            @Override
            public Void apply(CassandraClient client) throws Exception {
                int mutationBatchCount = config.mutationBatchCount();
                int mutationBatchSizeBytes = config.mutationBatchSizeBytes();
                long overrideTimestamp = Long.MIN_VALUE;
                if (overrideTimestamps) {
                    // Note: The timestamp is not needed on a non-sentinel code path.
                    overrideTimestamp = timestampOverrideSupplier.getAsLong();
                }

                for (List<Map.Entry<Cell, Value>> partition : IterablePartitioner.partitionByCountAndBytes(
                        values, mutationBatchCount, mutationBatchSizeBytes, tableRef, CellValuePutter::getEntrySize)) {
                    MutationMap map = new MutationMap();
                    for (Map.Entry<Cell, Value> e : partition) {
                        Cell cell = e.getKey();
                        Column col = overrideTimestamps
                                ? CassandraKeyValueServices.createColumnForDelete(cell, e.getValue(), overrideTimestamp)
                                : CassandraKeyValueServices.createColumn(cell, e.getValue());

                        ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
                        colOrSup.setColumn(col);
                        Mutation mutation = new Mutation();
                        mutation.setColumn_or_supercolumn(colOrSup);

                        map.addMutationForCell(cell, tableRef, mutation);
                    }

                    queryRunner.batchMutate(
                            kvsMethodName,
                            client,
                            ImmutableSet.of(tableRef),
                            map,
                            CassandraKeyValueServiceImpl.WRITE_CONSISTENCY);
                }
                return null;
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", " + Iterables.size(values)
                        + " values)";
            }
        });
    }
}
