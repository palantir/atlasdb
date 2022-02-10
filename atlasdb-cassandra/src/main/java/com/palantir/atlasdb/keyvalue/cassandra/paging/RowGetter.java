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
package com.palantir.atlasdb.keyvalue.cassandra.paging;

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.atlasdb.keyvalue.cassandra.pool.DcAwareHost;
import com.palantir.atlasdb.keyvalue.cassandra.thrift.SlicePredicates;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.UnavailableException;

public class RowGetter {
    private CassandraClientPool clientPool;
    private TracingQueryRunner queryRunner;
    private ConsistencyLevel consistency;
    private TableReference tableRef;

    public RowGetter(
            CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            ConsistencyLevel consistency,
            TableReference tableRef) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.consistency = consistency;
        this.tableRef = tableRef;
    }

    public List<KeySlice> getRows(String kvsMethodName, KeyRange keyRange, SlicePredicate slicePredicate) {
        DcAwareHost host = clientPool.getRandomHostForKey(keyRange.getStart_key());
        return clientPool.runWithRetryOnHost(host, new FunctionCheckedException<>() {
            @Override
            public List<KeySlice> apply(CassandraClient client) {
                try {
                    return queryRunner.run(
                            client,
                            tableRef,
                            () -> client.get_range_slices(
                                    kvsMethodName, tableRef, slicePredicate, keyRange, consistency));
                } catch (UnavailableException e) {
                    throw new InsufficientConsistencyException(
                            "get_range_slices requires " + consistency + " Cassandra nodes to be up and available.", e);
                } catch (Exception e) {
                    throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
                }
            }

            @Override
            public String toString() {
                return "get_range_slices(" + tableRef + ")";
            }
        });
    }

    public List<byte[]> getRowKeysInRange(byte[] rangeStart, byte[] rangeEnd, int maxResults) {
        KeyRange keyRange =
                new KeyRange().setStart_key(rangeStart).setEnd_key(rangeEnd).setCount(maxResults);
        SlicePredicate slicePredicate = SlicePredicates.create(SlicePredicates.Range.ALL, SlicePredicates.Limit.ZERO);

        List<KeySlice> rows = getRows("getRowKeysInRange", keyRange, slicePredicate);
        return rows.stream().map(KeySlice::getKey).collect(Collectors.toList());
    }
}
