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
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.MutationMap;
import com.palantir.atlasdb.qos.ratelimit.QosAwareThrowables;
import com.palantir.common.base.FunctionCheckedException;

class CellDeleter {
    private final Function<Long, Long> deleteTimestampGetter;

    private CassandraClientPool clientPool;
    private WrappingQueryRunner wrappingQueryRunner;
    private ConsistencyLevel deleteConsistency;

    CellDeleter(CassandraClientPool clientPool,
            WrappingQueryRunner wrappingQueryRunner,
            ConsistencyLevel deleteConsistency,
            Function<Long, Long> deleteTimestampGetter) {
        this.clientPool = clientPool;
        this.wrappingQueryRunner = wrappingQueryRunner;
        this.deleteConsistency = deleteConsistency;
        this.deleteTimestampGetter = deleteTimestampGetter;
    }

    void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        Map<InetSocketAddress, Map<Cell, Collection<Long>>> keysByHost = HostPartitioner.partitionMapByHost(clientPool,
                keys.asMap().entrySet());
        for (Map.Entry<InetSocketAddress, Map<Cell, Collection<Long>>> entry : keysByHost.entrySet()) {
            deleteOnSingleHost(entry.getKey(), tableRef, entry.getValue());
        }
    }

    private void deleteOnSingleHost(final InetSocketAddress host,
                                    final TableReference tableRef,
                                    final Map<Cell, Collection<Long>> cellVersionsMap) {
        try {
            clientPool.runWithRetryOnHost(host, new FunctionCheckedException<CassandraClient, Void, Exception>() {
                private int numVersions = 0;

                @Override
                public Void apply(CassandraClient client) throws Exception {
                    // Delete must delete in the order of timestamp and we don't trust batch_mutate to do it
                    // atomically so we have to potentially do many deletes if there are many timestamps for the
                    // same key.
                    Map<Integer, MutationMap> mutationMaps = Maps.newTreeMap();

                    for (Map.Entry<Cell, Collection<Long>> cellVersions : cellVersionsMap.entrySet()) {
                        int mapIndex = 0;
                        for (long ts : Ordering.natural().immutableSortedCopy(cellVersions.getValue())) {
                            if (!mutationMaps.containsKey(mapIndex)) {
                                mutationMaps.put(mapIndex, new MutationMap());
                            }
                            MutationMap mutationMap = mutationMaps.get(mapIndex);
                            ByteBuffer colName = CassandraKeyValueServices.makeCompositeBuffer(
                                    cellVersions.getKey().getColumnName(),
                                    ts);
                            SlicePredicate pred = new SlicePredicate();
                            pred.setColumn_names(Collections.singletonList(colName));
                            Deletion del = new Deletion();
                            del.setPredicate(pred);
                            del.setTimestamp(deleteTimestampGetter.apply(ts));
                            Mutation mutation = new Mutation();
                            mutation.setDeletion(del);

                            mutationMap.addMutationForCell(cellVersions.getKey(), tableRef, mutation);
                            mapIndex++;
                            numVersions += cellVersions.getValue().size();
                        }
                    }
                    for (MutationMap map : mutationMaps.values()) {
                        // NOTE: we run with ConsistencyLevel.ALL here instead of ConsistencyLevel.QUORUM
                        // because we want to remove all copies of this data
                        wrappingQueryRunner.batchMutate("delete", client, ImmutableSet.of(tableRef), map,
                                deleteConsistency);
                    }
                    return null;
                }

                @Override
                public String toString() {
                    return "delete_batch_mutate(" + host + ", " + tableRef.getQualifiedName() + ", "
                            + numVersions + " total versions of " + cellVersionsMap.size() + " keys)";
                }
            });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException("Deleting requires all Cassandra nodes to be up and available.",
                    e);
        } catch (Exception e) {
            throw QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(e);
        }
    }
}
