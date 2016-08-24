/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.UnavailableException;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.TracingQueryRunner;
import com.palantir.common.base.FunctionCheckedException;

public class RowGetter {

    private CassandraClientPool clientPool;
    private TracingQueryRunner queryRunner;
    private ConsistencyLevel consistency;
    private TableReference tableRef;
    private ColumnFetchMode fetchMode;

    public RowGetter(
            CassandraClientPool clientPool,
            TracingQueryRunner queryRunner,
            ConsistencyLevel consistency,
            TableReference tableRef,
            ColumnFetchMode fetchMode) {
        this.clientPool = clientPool;
        this.queryRunner = queryRunner;
        this.consistency = consistency;
        this.tableRef = tableRef;
        this.fetchMode = fetchMode;
    }

    public List<KeySlice> getRows(KeyRange keyRange) throws Exception {
        final ColumnParent colFam = new ColumnParent(CassandraKeyValueService.internalTableName(tableRef));
        InetSocketAddress host = clientPool.getRandomHostForKey(keyRange.getStart_key());
        return clientPool.runWithRetryOnHost(
                host,
                new FunctionCheckedException<Cassandra.Client, List<KeySlice>, Exception>() {
                    @Override
                    public List<KeySlice> apply(Cassandra.Client client) throws Exception {
                        try {
                            return queryRunner.run(client, tableRef,
                                    () -> client.get_range_slices(colFam, getSlicePredicate(), keyRange, consistency));
                        } catch (UnavailableException e) {
                            if (consistency.equals(ConsistencyLevel.ALL)) {
                                throw new InsufficientConsistencyException("This operation requires all Cassandra"
                                        + " nodes to be up and available.", e);
                            } else {
                                throw e;
                            }
                        }
                    }

                    @Override
                    public String toString() {
                        return "get_range_slices(" + colFam + ")";
                    }
                });
    }

    private SlicePredicate getSlicePredicate() {
        SliceRange slice = new SliceRange(
                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY),
                ByteBuffer.wrap(PtBytes.EMPTY_BYTE_ARRAY),
                false,
                fetchMode.getColumnsToFetch());
        final SlicePredicate predicate = new SlicePredicate();
        predicate.setSlice_range(slice);
        return predicate;
    }
}
