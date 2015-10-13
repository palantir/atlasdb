/**
 * Copyright 2015 Palantir Technologies
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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ExpiringKeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionManager;
import com.palantir.atlasdb.keyvalue.cassandra.jmx.CassandraJmxCompactionModule;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;

public class CassandraExpiringKeyValueService extends CassandraKeyValueService implements ExpiringKeyValueService{

    public static CassandraExpiringKeyValueService create(CassandraKeyValueServiceConfigManager configManager) {
        Optional<CassandraJmxCompactionManager> compactionManager = new CassandraJmxCompactionModule().createCompactionManager(configManager);
        return new CassandraExpiringKeyValueService(configManager, compactionManager);
    }

    protected CassandraExpiringKeyValueService(CassandraKeyValueServiceConfigManager configManager,
                                               Optional<CassandraJmxCompactionManager> compactionManager) {
        super(configManager, compactionManager);
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        Preconditions.checkState(!config.servers().isEmpty(), "address list was empty");
        try {
            initializeFromFreshInstance(containerPoolToUpdate.getCurrentHosts(), config.replicationFactor());
            getPoolingManager().submitHostRefreshTask();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void put(final String tableName, final Map<Cell, byte[]> values, final long timestamp, final long time, final TimeUnit unit) {
        try {
            putInternal(tableName, KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp), CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(String tableName, Multimap<Cell, Value> values, final long time, final TimeUnit unit) {
        try {
            putInternal(tableName, values.entries(), CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void multiPut(Map<String, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp, final long time, final TimeUnit unit) throws KeyAlreadyExistsException {
        List<Callable<Void>> callables = Lists.newArrayList();
        for (Entry<String, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final String table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, new Function<Entry<Cell, byte[]>, Long>(){

                @Override
                public Long apply(Entry<Cell, byte[]> entry) {
                    long totalSize = 0;
                    totalSize += entry.getValue().length;
                    totalSize += Cells.getApproxSizeOfCell(entry.getKey());
                    return totalSize;
                }});


            for (final List<Entry<Cell, byte[]>> p : partitions) {
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() {
                        Thread.currentThread().setName("Atlas expiry multiPut of " + p.size() + " cells into " + table);
                        put(table, Maps2.fromEntries(p), timestamp, time, unit);
                        return null;
                    }
                });
            }
        }

        List<Future<Void>> futures;
        try {
            futures = executor.invokeAll(callables);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        }
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw Throwables.throwUncheckedException(e);
            } catch (ExecutionException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        }
    }

}
