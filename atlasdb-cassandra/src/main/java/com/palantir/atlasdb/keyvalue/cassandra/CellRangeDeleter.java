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
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RetryLimitReachedException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.Mutations;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.thrift.TException;

class CellRangeDeleter {
    private final CassandraClientPool clientPool;
    private final WrappingQueryRunner wrappingQueryRunner;
    private final ConsistencyLevel deleteConsistency;
    private final LongUnaryOperator rangeTombstoneTimestampProvider;

    CellRangeDeleter(
            CassandraClientPool clientPool,
            WrappingQueryRunner wrappingQueryRunner,
            ConsistencyLevel deleteConsistency,
            LongUnaryOperator rangeTombstoneTimestampProvider) {
        this.clientPool = clientPool;
        this.wrappingQueryRunner = wrappingQueryRunner;
        this.deleteConsistency = deleteConsistency;
        this.rangeTombstoneTimestampProvider = rangeTombstoneTimestampProvider;
    }

    void deleteAllTimestamps(TableReference tableRef, Map<Cell, TimestampRangeDelete> deletes) {
        if (deletes.isEmpty()) {
            return;
        }

        Map<InetSocketAddress, Map<Cell, TimestampRangeDelete>> keysByHost =
                HostPartitioner.partitionMapByHost(clientPool, deletes.entrySet());

        // this is required by the interface of the CassandraMutationTimestampProvider, although it exists for tests
        long maxTimestampForAllCells = deletes.values().stream()
                .mapToLong(TimestampRangeDelete::timestamp)
                .max()
                .getAsLong();
        long rangeTombstoneCassandraTimestamp = rangeTombstoneTimestampProvider.applyAsLong(maxTimestampForAllCells);
        for (Map.Entry<InetSocketAddress, Map<Cell, TimestampRangeDelete>> entry : keysByHost.entrySet()) {
            deleteAllTimestampsOnSingleHost(
                    tableRef, entry.getKey(), entry.getValue(), rangeTombstoneCassandraTimestamp);
        }
    }

    private void deleteAllTimestampsOnSingleHost(
            TableReference tableRef,
            InetSocketAddress host,
            Map<Cell, TimestampRangeDelete> deletes,
            long rangeTombstoneCassandraTs) {
        if (deletes.isEmpty()) {
            return;
        }

        try {
            clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {

                @Override
                public Void apply(CassandraClient client) throws Exception {
                    insertRangeTombstones(client, deletes, tableRef, rangeTombstoneCassandraTs);
                    return null;
                }

                @Override
                public String toString() {
                    return "delete_timestamp_ranges_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                            + deletes.size() + " column timestamp ranges)";
                }
            });
        } catch (RetryLimitReachedException e) {
            throw CassandraUtils.wrapInIceForDeleteOrRethrow(e);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    /**
     * If we are deleting inclusive, we must first delete exclusive. This is because although we delete at consistency
     * all, this doesn't mean that the write fails if there are nodes down; it means that the write partially applies
     * if there are nodes down. This leads to a scenario where data can come back from the dead in some situations.
     * This can only affect us if there is no covering tombstone, so we make sure that we've deleted all historical
     * versions before deleting the latest cell (which is an Atlas level tombstone).
     */
    private void insertRangeTombstones(
            CassandraClient client,
            Map<Cell, TimestampRangeDelete> deletes,
            TableReference tableRef,
            long rangeTombstoneCassandraTs)
            throws TException {
        insertTombstones(client, deletes, tableRef, rangeTombstoneCassandraTs, TimestampRangeDelete::timestamp);
        insertTombstones(
                client,
                Maps.filterValues(deletes, TimestampRangeDelete::endInclusive),
                tableRef,
                rangeTombstoneCassandraTs,
                delete -> delete.maxTimestampToDelete() + 1);
    }

    private void insertTombstones(
            CassandraClient client,
            Map<Cell, TimestampRangeDelete> deletes,
            TableReference tableRef,
            long rangeTombstoneCassandraTs,
            ToLongFunction<TimestampRangeDelete> exclusiveMaxTimestampToDelete)
            throws TException {
        MutationMap mutationMap = new MutationMap();

        deletes.forEach((cell, delete) -> {
            Mutation mutation = getMutation(cell, delete, rangeTombstoneCassandraTs, exclusiveMaxTimestampToDelete);
            mutationMap.addMutationForCell(cell, tableRef, mutation);
        });

        wrappingQueryRunner.batchMutate(
                "deleteAllTimestamps", client, ImmutableSet.of(tableRef), mutationMap, deleteConsistency);
    }

    private Mutation getMutation(
            Cell cell,
            TimestampRangeDelete delete,
            long rangeTombstoneCassandraTimestamp,
            ToLongFunction<TimestampRangeDelete> exclusiveTimestampExtractor) {
        return Mutations.fromTimestampRangeDelete(
                cell.getColumnName(), delete, rangeTombstoneCassandraTimestamp, exclusiveTimestampExtractor);
    }
}
