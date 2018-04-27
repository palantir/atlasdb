/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ExpiringKeyValueService;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.Maps2;

public final class CassandraExpiringKeyValueService extends CassandraKeyValueServiceImpl
        implements ExpiringKeyValueService {
    public static CassandraExpiringKeyValueService create(
            CassandraKeyValueServiceConfigManager configManager,
            Optional<LeaderConfig> leaderConfig) {
        Preconditions.checkState(!configManager.getConfig().servers().isEmpty(), "address list was empty");

        CassandraExpiringKeyValueService kvs =
                new CassandraExpiringKeyValueService(configManager, leaderConfig,
                        AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        kvs.initialize(AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        return kvs;
    }

    private CassandraExpiringKeyValueService(
            CassandraKeyValueServiceConfigManager configManager,
            Optional<LeaderConfig> leaderConfig,
            boolean initializeAsync) {
        super(LoggerFactory.getLogger(CassandraKeyValueService.class), configManager, leaderConfig,
                initializeAsync);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp, long time, TimeUnit unit) {
        try {
            putInternal(
                    tableRef,
                    KeyValueServices.toConstantTimestampValues(values.entrySet(), timestamp),
                    CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void putWithTimestamps(TableReference tableRef, Multimap<Cell, Value> values, long time, TimeUnit unit) {
        try {
            putInternal(tableRef, values.entries(), CassandraKeyValueServices.convertTtl(time, unit));
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void multiPut(
            Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable,
            long timestamp,
            long time,
            TimeUnit unit) throws KeyAlreadyExistsException {
        List<Callable<Void>> callables = Lists.newArrayList();
        for (Entry<TableReference, ? extends Map<Cell, byte[]>> e : valuesByTable.entrySet()) {
            final TableReference table = e.getKey();
            // We sort here because some key value stores are more efficient if you store adjacent keys together.
            NavigableMap<Cell, byte[]> sortedMap = ImmutableSortedMap.copyOf(e.getValue());


            Iterable<List<Entry<Cell, byte[]>>> partitions = partitionByCountAndBytes(sortedMap.entrySet(),
                    getMultiPutBatchCount(), getMultiPutBatchSizeBytes(), table, entry ->
                            entry.getValue().length + Cells.getApproxSizeOfCell(entry.getKey()));

            for (final List<Entry<Cell, byte[]>> p : partitions) {
                callables.add(() -> {
                    Thread.currentThread().setName("Atlas expiry multiPut of " + p.size() + " cells into " + table);
                    put(table, Maps2.fromEntries(p), timestamp, time, unit);
                    return null;
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
