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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Write;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.IterablePartitioner;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.base.FunctionCheckedException;

public class CellValuePutter {
    private static final Function<Write, Long> ENTRY_SIZING_FUNCTION = input ->
            input.value().length + 4L + Cells.getApproxSizeOfCell(input.cell());

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

    void put(final String kvsMethodName, final Stream<Write> writes) {
        Map<InetSocketAddress, List<Write>> partitionedByHost = HostPartitioner.partitionByHost(clientPool,
                writes, write -> write.cell().getRowName());
        List<Callable<Void>> callables = new ArrayList<>();
        for (Map.Entry<InetSocketAddress, List<Write>> entry : partitionedByHost.entrySet()) {
            callables.addAll(getMultiPutTasksForSingleHost(kvsMethodName, entry.getKey(), entry.getValue()));
        }
        taskRunner.runAllTasksCancelOnFailure(callables);
    }

    private List<Callable<Void>> getMultiPutTasksForSingleHost(
            String kvsMethodName, InetSocketAddress host, List<Write> values) {
        Iterable<List<Write>> partitioned =
                IterablePartitioner.partitionByCountAndBytes(values,
                        config.mutationBatchCount(),
                        config.mutationBatchSizeBytes(),
                        extractTableNames(values).toString(),
                        ENTRY_SIZING_FUNCTION);
        List<Callable<Void>> tasks = Lists.newArrayList();
        for (final List<Write> batch : partitioned) {
            final Set<TableReference> tableRefs = extractTableNames(batch);
            tasks.add(AnnotatedCallable.wrapWithThreadName(AnnotationType.PREPEND,
                    "Atlas multiPut of " + batch.size() + " cells into " + tableRefs + " on " + host,
                    () -> multiPutForSingleHostInternal(kvsMethodName, host, tableRefs, batch)
            ));
        }
        return tasks;
    }

    private Void multiPutForSingleHostInternal(
            final String kvsMethodName,
            final InetSocketAddress host,
            final Set<TableReference> tableRefs,
            final List<Write> batch) throws Exception {
        final MutationMap mutationMap = convertToMutations(batch);
        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
            @Override
            public Void apply(CassandraClient client) throws Exception {
                return queryRunner.batchMutate(kvsMethodName, client, tableRefs, mutationMap,
                        writeConsistency);
            }

            @Override
            public String toString() {
                return "batch_mutate(" + host + ", " + tableRefs + ", " + batch.size() + " values)";
            }
        });
    }

    private MutationMap convertToMutations(List<Write> batch) {
        MutationMap mutationMap = new MutationMap();
        for (Write write : batch) {
            Cell cell = write.cell();
            Column col = CassandraKeyValueServices.createColumn(cell, write.timestamp(), write.value());

            ColumnOrSuperColumn colOrSup = new ColumnOrSuperColumn();
            colOrSup.setColumn(col);
            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(colOrSup);

            mutationMap.addMutationForCell(cell, write.table(), mutation);
        }
        return mutationMap;
    }

    private Set<TableReference> extractTableNames(List<Write> writes) {
        Set<TableReference> tableRefs = new HashSet<>();
        for (Write write : writes) {
            tableRefs.add(write.table());
        }
        return tableRefs;
    }
}
