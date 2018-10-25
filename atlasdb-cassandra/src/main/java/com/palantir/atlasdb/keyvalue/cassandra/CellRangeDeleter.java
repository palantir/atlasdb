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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.function.LongUnaryOperator;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.Mutations;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

class CellRangeDeleter {
    private final CassandraClientPool clientPool;
    private final WrappingQueryRunner wrappingQueryRunner;
    private final ConsistencyLevel deleteConsistency;
    private final LongUnaryOperator rangeTombstoneTimestampProvider;

    CellRangeDeleter(CassandraClientPool clientPool,
            WrappingQueryRunner wrappingQueryRunner,
            ConsistencyLevel deleteConsistency,
            LongUnaryOperator rangeTombstoneTimestampProvider) {
        this.clientPool = clientPool;
        this.wrappingQueryRunner = wrappingQueryRunner;
        this.deleteConsistency = deleteConsistency;
        this.rangeTombstoneTimestampProvider = rangeTombstoneTimestampProvider;
    }

    void deleteAllTimestamps(TableReference tableRef, Map<Cell, Long> maxTimestampExclusiveByCell,
            boolean deleteSentinels) {
        if (maxTimestampExclusiveByCell.isEmpty()) {
            return;
        }

        Map<InetSocketAddress, Map<Cell, Long>> keysByHost = HostPartitioner.partitionMapByHost(
                clientPool, maxTimestampExclusiveByCell.entrySet());

        // this is required by the interface of the CassandraMutationTimestampProvider, although it exists for tests
        long maxTimestampForAllCells = maxTimestampExclusiveByCell.values().stream()
                .mapToLong(x -> x).max().getAsLong();
        long rangeTombstoneCassandraTimestamp =
                rangeTombstoneTimestampProvider.applyAsLong(maxTimestampForAllCells);
        for (Map.Entry<InetSocketAddress, Map<Cell, Long>> entry : keysByHost.entrySet()) {
            deleteAllTimestampsOnSingleHost(
                    tableRef,
                    entry.getKey(),
                    entry.getValue(),
                    deleteSentinels,
                    rangeTombstoneCassandraTimestamp);
        }
    }

    private void deleteAllTimestampsOnSingleHost(
            TableReference tableRef,
            InetSocketAddress host,
            Map<Cell, Long> maxTimestampExclusiveByCell,
            boolean deleteSentinels,
            long rangeTombstoneCassandraTs) {
        if (maxTimestampExclusiveByCell.isEmpty()) {
            return;
        }

        try {
            clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {

                @Override
                public Void apply(CassandraClient client) throws Exception {
                    insertRangeTombstones(client, maxTimestampExclusiveByCell, tableRef,
                            deleteSentinels, rangeTombstoneCassandraTs);
                    return null;
                }

                @Override
                public String toString() {
                    return "delete_timestamp_ranges_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                            + maxTimestampExclusiveByCell.size() + " column timestamp ranges)";
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Deleting requires all Cassandra nodes to be up and available.",
                    e);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void insertRangeTombstones(CassandraClient client, Map<Cell, Long> maxTimestampExclusiveByCell,
            TableReference tableRef, boolean deleteSentinel, long rangeTombstoneCassandraTs) throws TException {
        MutationMap mutationMap = new MutationMap();

        maxTimestampExclusiveByCell.forEach((cell, maxTimestampExclusive) -> {
            Mutation mutation = getMutation(cell, maxTimestampExclusive, deleteSentinel, rangeTombstoneCassandraTs);

            mutationMap.addMutationForCell(cell, tableRef, mutation);
        });

        wrappingQueryRunner.batchMutate("deleteAllTimestamps", client, ImmutableSet.of(tableRef), mutationMap,
                deleteConsistency);
    }

    private Mutation getMutation(Cell cell, long maxTimestampExclusive,
            boolean deleteSentinel, long rangeTombstoneCassandraTimestamp) {
        if (deleteSentinel) {
            return Mutations.rangeTombstoneIncludingSentinelForColumn(cell.getColumnName(), maxTimestampExclusive,
                    rangeTombstoneCassandraTimestamp);
        }
        return Mutations.rangeTombstoneForColumn(
                cell.getColumnName(),
                maxTimestampExclusive,
                rangeTombstoneCassandraTimestamp);
    }
}
