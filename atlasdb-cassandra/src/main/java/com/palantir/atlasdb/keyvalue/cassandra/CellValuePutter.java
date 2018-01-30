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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

public class CellValuePutter {
    private static final Function<Map.Entry<Cell, Value>, Long> ENTRY_SIZING_FUNCTION = input ->
            input.getValue().getContents().length + 4L + Cells.getApproxSizeOfCell(input.getKey());

    private CassandraKeyValueServiceConfig config;
    private CassandraClientPool clientPool;
    private TaskRunner taskRunner;
    private WrappingQueryRunner queryRunner;
    private ConsistencyLevel writeConsistency;

    public CellValuePutter(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            TaskRunner taskRunner,
            WrappingQueryRunner queryRunner,
            ConsistencyLevel writeConsistency) {
        this.config = config;
        this.clientPool = clientPool;
        this.taskRunner = taskRunner;
        this.queryRunner = queryRunner;
        this.writeConsistency = writeConsistency;
    }

    void put(final String kvsMethodName,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values) throws Exception {
        Map<InetSocketAddress, Map<Cell, Value>> cellsByHost = new HostPartitioner<Value>(clientPool)
                .partitionMapByHost(values);
        List<Callable<Void>> tasks = Lists.newArrayListWithCapacity(cellsByHost.size());
        for (final Map.Entry<InetSocketAddress, Map<Cell, Value>> entry : cellsByHost.entrySet()) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas put " + entry.getValue().size()
                            + " cell values to " + tableRef + " on " + entry.getKey(),
                    () -> {
                        putForSingleHost(kvsMethodName, entry.getKey(), tableRef, entry.getValue().entrySet());
                        clientPool.markWritesForTable(entry.getValue(), tableRef);
                        return null;
                    }));
        }
        taskRunner.runAllTasksCancelOnFailure(tasks);
    }

    private void putForSingleHost(String kvsMethodName,
            final InetSocketAddress host,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values) throws Exception {
        clientPool.runWithRetryOnHost(host,
                new FunctionCheckedException<CassandraClient, Void, Exception>() {
                    @Override
                    public Void apply(CassandraClient client) throws Exception {
                        int mutationBatchCount = config.mutationBatchCount();
                        int mutationBatchSizeBytes = config.mutationBatchSizeBytes();
                        for (List<Map.Entry<Cell, Value>> partition : IterablePartitioner.partitionByCountAndBytes(
                                values,
                                mutationBatchCount,
                                mutationBatchSizeBytes,
                                tableRef,
                                ENTRY_SIZING_FUNCTION)) {
                            MutationMap map = new MutationMap();
                            for (Map.Entry<Cell, Value> e : partition) {
                                Cell cell = e.getKey();
                                Column col = CassandraKeyValueServices.createColumn(cell, e.getValue());

                                ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
                                colOrSup.setColumn(col);
                                Mutation mutation = new Mutation();
                                mutation.setColumn_or_supercolumn(colOrSup);

                                map.addMutationForCell(cell, tableRef, mutation);
                            }

                            queryRunner.batchMutate(kvsMethodName, client, ImmutableSet.of(tableRef), map,
                                    writeConsistency);
                        }
                        return null;
                    }

                    @Override
                    public String toString() {
                        return "batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                                + Iterables.size(values) + " values)";
                    }
                });
    }
}
