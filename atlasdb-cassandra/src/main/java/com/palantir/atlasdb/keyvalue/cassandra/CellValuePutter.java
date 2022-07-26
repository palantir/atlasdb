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
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.IterablePartitioner;
import com.palantir.atlasdb.pue.PueKvsConsensusForgettingStore;
import com.palantir.atlasdb.util.AnnotatedCallable;
import com.palantir.atlasdb.util.AnnotationType;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.refreshable.Refreshable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;

public class CellValuePutter {
    /**
     * This value has been chosen so that, in case of internal KVS inconsistency, the value stored with
     * {@link PueKvsConsensusForgettingStore#put(Cell, byte[])} is always considered as the latest value. It is the
     * responsibility of the user of this class to verify that this is true for the particular KVS implementation,
     * which it is and must remain so for the Cassandra KVS.
     */
    public static final long SET_TIMESTAMP = Long.MAX_VALUE - 10;

    private final LongSupplier timestampOverrideSupplier;

    private final Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig;
    private final CassandraClientPool clientPool;
    private final TaskRunner taskRunner;
    private final WrappingQueryRunner queryRunner;

    public CellValuePutter(
            Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig,
            CassandraClientPool clientPool,
            TaskRunner taskRunner,
            WrappingQueryRunner queryRunner,
            LongSupplier timestampOverrideSupplier) {
        this.runtimeConfig = runtimeConfig;
        this.clientPool = clientPool;
        this.taskRunner = taskRunner;
        this.queryRunner = queryRunner;
        this.timestampOverrideSupplier = timestampOverrideSupplier;
    }

    void putWithOverriddenTimestamps(
            final String kvsMethodName, final TableReference tableRef, final Iterable<Map.Entry<Cell, Value>> values) {
        putInternal(kvsMethodName, tableRef, values, Optional.of(timestampOverrideSupplier.getAsLong()));
    }

    void put(final String kvsMethodName, final TableReference tableRef, final Iterable<Map.Entry<Cell, Value>> values) {
        putInternal(kvsMethodName, tableRef, values, Optional.empty());
    }

    void set(final String kvsMethodName, final TableReference tableRef, final Iterable<Map.Entry<Cell, Value>> values) {
        putInternal(kvsMethodName, tableRef, values, Optional.of(SET_TIMESTAMP));
    }

    /**
     * @param values the values to put. The timestamp of each value is the AtlasDB start timestamp, which is a part of
     *               the column name in Cassandra.
     * @param overrideTimestamp the Cassandra timestamp to write the value at. A higher Cassandra timestamp determines
     *                          which write wins in case of a discrepancy on multiple nodes. If empty, defaults to the
     *                          start timestamp from above.
     */
    private void putInternal(
            final String kvsMethodName,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values,
            Optional<Long> overrideTimestamp) {
        Map<CassandraServer, Map<Cell, Value>> cellsByHost = HostPartitioner.partitionMapByHost(clientPool, values);
        List<Callable<Void>> tasks = new ArrayList<>(cellsByHost.size());
        for (final Map.Entry<CassandraServer, Map<Cell, Value>> entry : cellsByHost.entrySet()) {
            tasks.add(AnnotatedCallable.wrapWithThreadName(
                    AnnotationType.PREPEND,
                    "Atlas put " + entry.getValue().size() + " cell values to " + tableRef + " on "
                            + entry.getKey().cassandraHostName(),
                    () -> {
                        putForSingleHost(
                                kvsMethodName,
                                entry.getKey(),
                                tableRef,
                                entry.getValue().entrySet(),
                                overrideTimestamp);
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
            final CassandraServer server,
            final TableReference tableRef,
            final Iterable<Map.Entry<Cell, Value>> values,
            Optional<Long> overrideTimestamp)
            throws Exception {
        clientPool.runWithRetryOnServer(server, new FunctionCheckedException<CassandraClient, Void, Exception>() {
            @Override
            public Void apply(CassandraClient client) throws Exception {
                int mutationBatchCount = runtimeConfig.get().mutationBatchCount();
                int mutationBatchSizeBytes = runtimeConfig.get().mutationBatchSizeBytes();

                for (List<Map.Entry<Cell, Value>> partition : IterablePartitioner.partitionByCountAndBytes(
                        values, mutationBatchCount, mutationBatchSizeBytes, tableRef, CellValuePutter::getEntrySize)) {
                    MutationMap map = new MutationMap();
                    for (Map.Entry<Cell, Value> e : partition) {
                        Cell cell = e.getKey();
                        Column col = overrideTimestamp
                                .map(ts -> CassandraKeyValueServices.createColumnForDelete(cell, e.getValue(), ts))
                                .orElseGet(() -> CassandraKeyValueServices.createColumn(cell, e.getValue()));

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
                return "batch_mutate(" + server.cassandraHostName() + ", " + tableRef.getQualifiedName() + ", "
                        + Iterables.size(values) + " values)";
            }
        });
    }
}
