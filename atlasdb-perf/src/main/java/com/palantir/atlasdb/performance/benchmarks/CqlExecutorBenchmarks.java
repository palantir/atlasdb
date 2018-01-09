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

package com.palantir.atlasdb.performance.benchmarks;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutor;
import com.palantir.atlasdb.keyvalue.cassandra.CqlExecutorImpl;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.performance.benchmarks.table.ConsecutiveNarrowTable;

// Obviously, this will only work on CKVS
@State(Scope.Benchmark)
public class CqlExecutorBenchmarks {

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object getTimestampsUsingInClause(ConsecutiveNarrowTable.AverageTable table) {
        CqlExecutor cqlExecutor = getCqlExecutor(table);

        List<byte[]> rows = table.getRowList().subList(0, 1024);
        int limit = 1000;
        List<CellWithTimestamp> cells = cqlExecutor.getTimestamps(table.getTableRef(), rows, limit);

        Preconditions.checkState(cells.size() == limit, "Should have gotten 1000 cells back");

        return cells;
    }

    @Benchmark
    @Threads(1)
    @Warmup(time = 3, timeUnit = TimeUnit.SECONDS)
    @Measurement(time = 15, timeUnit = TimeUnit.SECONDS)
    public Object getTimestampsUsingGreaterThan(ConsecutiveNarrowTable.AverageTable table) {
        CqlExecutor cqlExecutor = getCqlExecutor(table);

        List<byte[]> rows = table.getRowList();
        byte[] firstRow = rows.get(0);
        byte[] lastRow = rows.get(1023);
        int limit = 1000;
        List<CellWithTimestamp> cells = cqlExecutor.getTimestamps(table.getTableRef(), firstRow, lastRow, limit);

        Preconditions.checkState(cells.size() == limit, "Should have gotten 1000 cells back");

        return cells;
    }

    private CqlExecutor getCqlExecutor(ConsecutiveNarrowTable.AverageTable table) {
        KeyValueService kvs = table.getKvs();
        CassandraKeyValueServiceImpl ckvs = getCKVS(kvs);
        CassandraClientPool clientPool = ckvs.getClientPool();
        return new CqlExecutorImpl(clientPool, ConsistencyLevel.QUORUM);
    }

    private CassandraKeyValueServiceImpl getCKVS(KeyValueService kvs) {
        Collection<? extends KeyValueService> delegates = kvs.getDelegates();
        String delegateNames = delegates.stream().map(del -> del.getClass().getCanonicalName()).collect(
                Collectors.joining(","));
        return delegates.stream()
                .filter(delegate -> delegate instanceof CassandraKeyValueServiceImpl)
                .map(instance -> (CassandraKeyValueServiceImpl) instance)
                .findAny()
                .orElseThrow(() -> new NullPointerException(delegateNames));
    }

}
