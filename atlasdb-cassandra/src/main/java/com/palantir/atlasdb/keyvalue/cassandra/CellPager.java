/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.util.paging.Pager;

public class CellPager {

    private final CassandraClientPool clientPool;
    private final InetSocketAddress host;

    public CellPager(CassandraClientPool clientPool, InetSocketAddress host) {
        this.clientPool = clientPool;
        this.host = host;
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> getColsByKeyWithPaging(Set<ByteBuffer> rows, TableReference tableRef, ConsistencyLevel consistency) throws TException {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> colsByKey = new HashMap<>();

        for (ByteBuffer row : rows) {
            CqlExecutor cqlExecutor = new CqlExecutor(clientPool, host, consistency);
            AllCellsPerRowPager allCellsPerRowPager = new AllCellsPerRowPager(cqlExecutor, row, tableRef, 10);
            List<ColumnOrSuperColumn> columns = new Pager<>(allCellsPerRowPager).getPages();
            colsByKey.put(row, columns);
        }
        return colsByKey;
    }
}
